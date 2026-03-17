"""日线智能增量更新（含数据清洗）。

能力：
- 自动检测需更新股票并并发下载
- 支持 --dry-run / --limit-stocks
- 合并后统一执行清洗规则，避免再次引入脏数据
"""

from __future__ import annotations

import argparse
import datetime
import io
import os
import sys
import tempfile
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import baostock as bs
import multiprocessing as mp
import pandas as pd

from 配置 import (
    BAOSTOCK_FIELDS,
    原始CSV目录,
    合并数据路径,
    同步更新CSV,
    执行时间,
    复权方式,
    输出列名,
    日线并发进程数,
    频率,
)
from 数据清洗规则 import 原子写入Parquet, 打印报告, 清洗日线


BAOSTOCK_LOCK = Path(tempfile.gettempdir()) / "baostock.lock"
并发数 = 日线并发进程数
批任务大小 = 80
最大重试次数 = 3
重试基础间隔 = 1


def _完整交易日截止时间() -> datetime.time:
    try:
        hour_str, minute_str = 执行时间.split(":")
        return datetime.time(int(hour_str), int(minute_str))
    except Exception:
        return datetime.time(18, 0)


def _有效股票代码(series: pd.Series) -> pd.Series:
    s = series.fillna("").astype(str).str.strip()
    return s.str.fullmatch(r"\d{6}", na=False)


def 代码转BaoStock格式(stock_code: str) -> str:
    if stock_code.startswith(("6", "9")):
        return f"sh.{stock_code}"
    return f"sz.{stock_code}"


def 获取交易日历(开始日期: str, 结束日期: str) -> list[str]:
    rs = bs.query_trade_dates(start_date=开始日期, end_date=结束日期)
    out: list[str] = []
    while rs.next():
        d, is_trade = rs.get_row_data()
        if is_trade == "1":
            out.append(d)
    return sorted(out)


def _worker_download_batch(tasks: list[dict]) -> list[dict]:
    import baostock as bs

    def _静默登录() -> object:
        old_stdout_inner = sys.stdout
        sys.stdout = io.StringIO()
        try:
            return bs.login()
        finally:
            sys.stdout = old_stdout_inner

    def _静默登出() -> None:
        old_stdout_inner = sys.stdout
        sys.stdout = io.StringIO()
        try:
            bs.logout()
        except Exception:
            pass
        finally:
            sys.stdout = old_stdout_inner

    try:
        devnull_fd = os.open(os.devnull, os.O_WRONLY)
        old_stderr = os.dup(2)
        os.dup2(devnull_fd, 2)
    except Exception:
        devnull_fd = None
        old_stderr = None

    lg = _静默登录()

    if lg.error_code != "0":
        if devnull_fd is not None:
            try:
                os.dup2(old_stderr, 2)
                os.close(old_stderr)
                os.close(devnull_fd)
            except Exception:
                pass
        return [
            {
                "ok": False,
                "stock_code": t["stock_code"],
                "stock_name": t["stock_name"],
                "last_date": t["last_date"],
                "error": "login failed",
            }
            for t in tasks
        ]

    results: list[dict] = []
    for task in tasks:
        code = task["stock_code"]
        name = task["stock_name"]
        last_date = task["last_date"]
        start_date = task["start_date"]
        end_date = task["end_date"]

        rows = []
        fields = None
        err = None
        bs_code = 代码转BaoStock格式(code)

        for i in range(最大重试次数):
            try:
                rs = bs.query_history_k_data_plus(
                    code=bs_code,
                    fields=BAOSTOCK_FIELDS,
                    start_date=start_date,
                    end_date=end_date,
                    frequency=频率,
                    adjustflag=复权方式,
                )
                if rs.error_code != "0":
                    err = rs.error_msg
                    if "用户未登录" in err:
                        _静默登出()
                        lg = _静默登录()
                        if lg.error_code == "0":
                            continue
                    if i < 最大重试次数 - 1:
                        time.sleep(重试基础间隔 * (2**i))
                        continue
                    break

                fields = rs.fields
                while rs.next():
                    rows.append(rs.get_row_data())
                break
            except Exception as e:
                err = str(e)
                if i < 最大重试次数 - 1:
                    time.sleep(重试基础间隔 * (2**i))

        if rows:
            results.append(
                {
                    "ok": True,
                    "stock_code": code,
                    "stock_name": name,
                    "last_date": last_date,
                    "fields": fields,
                    "rows": rows,
                }
            )
        else:
            results.append(
                {
                    "ok": False,
                    "stock_code": code,
                    "stock_name": name,
                    "last_date": last_date,
                    "error": err or "no data",
                }
            )

    _静默登出()

    if devnull_fd is not None:
        try:
            os.dup2(old_stderr, 2)
            os.close(old_stderr)
            os.close(devnull_fd)
        except Exception:
            pass

    return results


def BaoStock数据转标准格式(df: pd.DataFrame, stock_code: str, stock_name: str) -> pd.DataFrame:
    out = pd.DataFrame(index=df.index)
    out["date"] = df["date"]
    out["stock_code"] = stock_code
    out["stock_name"] = stock_name
    out["open"] = pd.to_numeric(df["open"], errors="coerce")
    out["high"] = pd.to_numeric(df["high"], errors="coerce")
    out["low"] = pd.to_numeric(df["low"], errors="coerce")
    out["close"] = pd.to_numeric(df["close"], errors="coerce")
    out["volume"] = pd.to_numeric(df["volume"], errors="coerce")
    out["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    out["outstanding_share"] = 0.0
    out["turnover"] = pd.to_numeric(df["turn"], errors="coerce")
    return out[输出列名]


def 追加到CSV(新数据: pd.DataFrame, stock_code: str, stock_name: str) -> None:
    匹配文件 = list(原始CSV目录.glob(f"{stock_code}_*.csv"))
    csv列 = ["open", "high", "low", "close", "volume", "amount", "outstanding_share", "turnover", "date"]

    if not 匹配文件:
        文件路径 = 原始CSV目录 / f"{stock_code}_{stock_name}.csv"
        新数据[csv列].to_csv(文件路径, index=False)
        return

    文件路径 = 匹配文件[0]
    新数据[csv列].to_csv(文件路径, mode="a", header=False, index=False)


def 检查BaoStock锁() -> bool:
    if BAOSTOCK_LOCK.exists():
        mtime = BAOSTOCK_LOCK.stat().st_mtime
        if time.time() - mtime < 1800:
            return True
        BAOSTOCK_LOCK.unlink(missing_ok=True)
    return False


def 创建BaoStock锁() -> None:
    BAOSTOCK_LOCK.parent.mkdir(parents=True, exist_ok=True)
    BAOSTOCK_LOCK.write_text(f"daily-update {datetime.datetime.now().isoformat()}", encoding="utf-8")


def 释放BaoStock锁() -> None:
    BAOSTOCK_LOCK.unlink(missing_ok=True)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="日线智能更新")
    p.add_argument("--dry-run", action="store_true", help="只拉取和校验，不写回文件")
    p.add_argument("--limit-stocks", type=int, default=0, help="调试：最多更新前N只（按最滞后优先）")
    return p.parse_args()


def 智能更新(dry_run: bool, limit_stocks: int) -> None:
    print("=" * 70)
    print(f"日线智能更新 - {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)
    print(f"dry_run={dry_run}, limit_stocks={limit_stocks}")

    if 检查BaoStock锁():
        print(f"[警告] 检测到其他BaoStock脚本运行中: {BAOSTOCK_LOCK}")
        return

    if not 合并数据路径.exists():
        print(f"[错误] 日线文件不存在: {合并数据路径}")
        return

    创建BaoStock锁()
    try:
        print("\n[1/6] 读取并清洗现有日线数据...")
        现有原始 = pd.read_parquet(合并数据路径)
        现有数据, 清洗报告 = 清洗日线(现有原始)
        打印报告("现有日线清洗摘要", 清洗报告)

        print("\n[2/6] 计算每只股票最后日期...")
        按股最后日 = 现有数据.groupby("stock_code", as_index=False)["date"].max().rename(columns={"date": "last_date"})
        最新名称 = (
            现有数据.sort_values(["stock_code", "date"], kind="mergesort")
            .drop_duplicates(subset=["stock_code"], keep="last")
            .loc[:, ["stock_code", "stock_name"]]
        )
        股票最新日期 = 按股最后日.merge(最新名称, on="stock_code", how="left")
        股票最新日期 = 股票最新日期[_有效股票代码(股票最新日期["stock_code"])].copy()

        print("\n[3/6] 获取最近完整交易日...")
        lg = bs.login()
        if lg.error_code != "0":
            print(f"[错误] BaoStock登录失败: {lg.error_msg}")
            return

        今天 = datetime.date.today().strftime("%Y-%m-%d")
        最早 = 股票最新日期["last_date"].min()
        交易日 = 获取交易日历(最早, 今天)
        bs.logout()

        if not 交易日:
            print("[错误] 未获取到交易日")
            return

        截止时间 = _完整交易日截止时间()
        if datetime.datetime.now().time() < 截止时间 and 交易日[-1] == 今天:
            交易日 = 交易日[:-1]

        if not 交易日:
            print("[错误] 当前时间早于完整交易日截止时间，且无可用历史交易日")
            return

        最近交易日 = 交易日[-1]
        需要更新 = 股票最新日期[股票最新日期["last_date"] < 最近交易日].copy()
        需要更新 = 需要更新.sort_values(["last_date", "stock_code"]).reset_index(drop=True)

        if limit_stocks > 0:
            需要更新 = 需要更新.head(limit_stocks).copy()
            print(f"  [调试] 限制更新股票数: {len(需要更新)}")

        print(f"  最近交易日: {最近交易日}")
        print(f"  需更新股票: {len(需要更新)}")

        if 需要更新.empty:
            print("\n所有股票均已是最新，无需更新。")
            return

        print(f"\n[4/6] 并发下载增量数据（{并发数} 进程）...")
        tasks = [
            {
                "stock_code": r["stock_code"],
                "stock_name": r["stock_name"],
                "last_date": r["last_date"],
                "start_date": r["last_date"],
                "end_date": 最近交易日,
            }
            for _, r in 需要更新.iterrows()
        ]

        batches = [tasks[i : i + 批任务大小] for i in range(0, len(tasks), 批任务大小)]

        新数据列表: list[pd.DataFrame] = []
        失败列表: list[tuple[str, str, str]] = []
        完成 = 0
        总任务 = len(tasks)

        with ProcessPoolExecutor(max_workers=并发数) as ex:
            fut_map = {ex.submit(_worker_download_batch, b): b for b in batches}
            for fut in as_completed(fut_map):
                batch_tasks = fut_map[fut]
                try:
                    results = fut.result()
                except Exception as e:
                    for t in batch_tasks:
                        失败列表.append((t["stock_code"], t["stock_name"], f"batch error: {e}"))
                        完成 += 1
                    print(f"  进度: {完成}/{总任务}")
                    continue

                for r in results:
                    完成 += 1
                    code = r["stock_code"]
                    name = r["stock_name"]
                    last_date = r["last_date"]

                    if not r.get("ok"):
                        err = r.get("error", "unknown")
                        if err != "no data":
                            失败列表.append((code, name, err))
                        if 完成 % 50 == 0 or 完成 == 总任务:
                            print(f"  进度: {完成}/{总任务}")
                        continue

                    raw = pd.DataFrame(r["rows"], columns=r["fields"])
                    std = BaoStock数据转标准格式(raw, code, name)
                    std = std[std["date"] > last_date]
                    std = std[_有效股票代码(std["stock_code"])]
                    if not std.empty:
                        新数据列表.append(std)

                    if 完成 % 50 == 0 or 完成 == 总任务:
                        print(f"  进度: {完成}/{总任务}")

        if not 新数据列表:
            print("\n未获得新增日线数据。")
            if 失败列表:
                print(f"失败股票数: {len(失败列表)}")
            return

        print("\n[5/6] 合并并清洗结果...")
        新增合并 = pd.concat(新数据列表, ignore_index=True)
        合并后 = pd.concat([现有数据, 新增合并], ignore_index=True)
        合并清洗后, 合并清洗报告 = 清洗日线(合并后)
        打印报告("合并后日线清洗摘要", 合并清洗报告)

        if 同步更新CSV and not dry_run:
            print("\n  同步更新原始CSV...")
            for code, grp in 新增合并.groupby("stock_code"):
                stock_name = str(grp["stock_name"].iloc[-1]) if not grp.empty else ""
                追加到CSV(grp, code, stock_name)

        print("\n[6/6] 写回文件...")
        print(f"  新增行数: {len(新增合并):,}")
        print(f"  更新后总行数: {len(合并清洗后):,}")
        print(f"  失败股票数: {len(失败列表)}")

        if 失败列表:
            print("  失败样本（前20）:")
            for code, name, err in 失败列表[:20]:
                print(f"    {code} {name}: {err}")

        if dry_run:
            print("\n[DRY-RUN] 不写回Parquet")
            return

        原子写入Parquet(合并清洗后, 合并数据路径)
        print(f"[OK] 已写回: {合并数据路径}")

    finally:
        释放BaoStock锁()


def main() -> None:
    args = parse_args()
    智能更新(dry_run=args.dry_run, limit_stocks=max(0, args.limit_stocks))


if __name__ == "__main__":
    mp.freeze_support()
    main()
