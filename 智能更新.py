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
import signal
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
逐股验收再跑轮数 = 1
最低覆盖率 = 0.5
最低覆盖股票数 = 1000
单股查询超时秒数 = 120


def _完整交易日截止时间() -> datetime.time:
    try:
        hour_str, minute_str = 执行时间.split(":")
        return datetime.time(int(hour_str), int(minute_str))
    except Exception:
        return datetime.time(18, 0)


def 规范化查询日期(value: object) -> str | None:
    if value is None or pd.isna(value):
        return None
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        text = str(value).strip()
        return text[:10] if len(text) >= 10 else None
    return ts.strftime("%Y-%m-%d")


def _打印逐股验收(
    待更新: pd.DataFrame,
    更新后最新: pd.DataFrame,
    最近交易日: str,
    源端无新增代码: dict[str, str],
) -> pd.DataFrame:
    验收 = 待更新.loc[:, ["stock_code", "stock_name"]].merge(
        更新后最新.loc[:, ["stock_code", "last_date"]],
        on="stock_code",
        how="left",
    )
    验收["status"] = "ok"
    no_newer = set(源端无新增代码)
    lagging_mask = 验收["last_date"].isna() | (验收["last_date"] < 最近交易日)
    验收.loc[lagging_mask, "status"] = "lagging"
    # 对已确认“源端无更晚数据”的股票，跳过最终失败判定
    验收.loc[验收["stock_code"].isin(no_newer), "status"] = "source_no_newer"

    print("\n[验收] 逐股更新结果:")
    print(f"  待验收股票数: {len(验收)}")
    print(f"  已到目标交易日: {int((验收['status'] == 'ok').sum())}")
    print(f"  源端无更晚数据: {int((验收['status'] == 'source_no_newer').sum())}")
    print(f"  仍落后目标日: {int((验收['status'] == 'lagging').sum())}")

    def _fmt_date(value: object) -> str:
        return str(value) if pd.notna(value) else "-"

    lagging = 验收[验收["status"] == "lagging"].copy()
    if not lagging.empty:
        print("  落后样本（前20）:")
        for _, row in lagging.head(20).iterrows():
            print(
                f"    {row['stock_code']} {row['stock_name']}: "
                f"last_date={_fmt_date(row['last_date'])} target={最近交易日}"
            )

    source_no_newer = 验收[验收["status"] == "source_no_newer"].copy()
    if not source_no_newer.empty:
        print("  源端无更晚数据样本（前20）:")
        for _, row in source_no_newer.head(20).iterrows():
            print(
                f"    {row['stock_code']} {row['stock_name']}: "
                f"last_date={_fmt_date(row['last_date'])} target={最近交易日}"
            )

    return 验收


def _检查目标日覆盖率(
    验收: pd.DataFrame,
    *,
    最近交易日: str,
    总股票数: int,
) -> None:
    已到目标日 = int((验收["status"] == "ok").sum())
    最低需要 = max(最低覆盖股票数, int(总股票数 * 最低覆盖率))
    if 已到目标日 < 最低需要:
        print("[错误] 最新交易日覆盖率过低，视为本次更新失败")
        print(f"  目标日: {最近交易日}")
        print(f"  已到目标日: {已到目标日}")
        print(f"  总股票数: {总股票数}")
        print(f"  最低要求: {最低需要}")
        raise SystemExit(2)


def _按股票代码去重保留最后(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    return (
        df.sort_values(["stock_code", "date"], kind="mergesort")
        .drop_duplicates(subset=["stock_code", "date"], keep="last")
        .reset_index(drop=True)
    )


def _针对滞后股票补跑(
    合并清洗后: pd.DataFrame,
    滞后股票: pd.DataFrame,
    最近交易日: str,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, str]]:
    if 滞后股票.empty:
        return 合并清洗后, 滞后股票.copy(), {}

    latest_map = (
        合并清洗后.groupby("stock_code", as_index=False)["date"]
        .max()
        .rename(columns={"date": "last_date"})
    )
    latest_map["last_date"] = latest_map["last_date"].map(规范化查询日期)
    latest_map = latest_map[latest_map["stock_code"].isin(滞后股票["stock_code"].astype(str))].copy()
    if latest_map.empty:
        return 合并清洗后, 滞后股票.copy(), {}

    latest_name = (
        合并清洗后.sort_values(["stock_code", "date"], kind="mergesort")
        .drop_duplicates(subset=["stock_code"], keep="last")
        .loc[:, ["stock_code", "stock_name"]]
    )
    latest_map = latest_map.merge(latest_name, on="stock_code", how="left")

    lagging_tasks = [
        {
            "stock_code": r["stock_code"],
            "stock_name": r["stock_name"],
            "last_date": r["last_date"],
            "start_date": r["last_date"],
                "end_date": 最近交易日,
            }
        for _, r in latest_map.iterrows()
        if isinstance(r["last_date"], str) and r["last_date"]
    ]
    if not lagging_tasks:
        return 合并清洗后, 滞后股票.copy(), {}

    print(f"\n[补跑] 针对 {len(lagging_tasks)} 只落后股票立即再跑一轮")
    batches = [lagging_tasks[i : i + 批任务大小] for i in range(0, len(lagging_tasks), 批任务大小)]
    新数据列表: list[pd.DataFrame] = []
    失败列表: list[tuple[str, str, str]] = []
    源端无新增代码: dict[str, str] = {}
    完成 = 0
    总任务 = len(lagging_tasks)

    with ProcessPoolExecutor(max_workers=并发数) as ex:
        fut_map = {ex.submit(_worker_download_batch, batch): batch for batch in batches}
        for fut in as_completed(fut_map):
            batch_tasks = fut_map[fut]
            try:
                results = fut.result()
            except Exception as e:
                for t in batch_tasks:
                    失败列表.append((t["stock_code"], t["stock_name"], f"batch error: {e}"))
                    完成 += 1
                print(f"  [补跑进度] {完成}/{总任务}")
                continue

            for r in results:
                完成 += 1
                code = r["stock_code"]
                name = r["stock_name"]
                if not r.get("ok"):
                    err = r.get("error", "unknown")
                    if err == "no data":
                        源端无新增代码[code] = name
                    else:
                        失败列表.append((code, name, err))
                    continue

                t = next((item for item in batch_tasks if item["stock_code"] == code), None)
                df = BaoStock数据转标准格式(pd.DataFrame(r["rows"], columns=r["fields"]), code, name)
                last_d = t.get("last_date") if t else None
                if last_d:
                    df = df[df["date"] > last_d]
                if not df.empty:
                    新数据列表.append(df)
                else:
                    源端无新增代码[code] = name

    if 新数据列表:
        补跑合并 = pd.concat(新数据列表, ignore_index=True)
        合并清洗后 = pd.concat([合并清洗后, 补跑合并], ignore_index=True)
        合并清洗后, _ = 清洗日线(合并清洗后)
    if 失败列表:
        print(f"  [补跑] 仍有失败股票数: {len(失败列表)}")
    return 合并清洗后, 滞后股票.copy(), 源端无新增代码


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

    def _超时处理(signum, frame):  # noqa: ARG001
        raise TimeoutError(f"baostock query timed out after {单股查询超时秒数}s")

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
        print(f"[worker] query {code} {name} {start_date}->{end_date}", flush=True)

        for i in range(最大重试次数):
            try:
                signal.signal(signal.SIGALRM, _超时处理)
                signal.alarm(单股查询超时秒数)
                rs = bs.query_history_k_data_plus(
                    code=bs_code,
                    fields=BAOSTOCK_FIELDS,
                    start_date=start_date,
                    end_date=end_date,
                    frequency=频率,
                    adjustflag=复权方式,
                )
                if rs is None:
                    err = "query returned none"
                    if i < 最大重试次数 - 1:
                        time.sleep(重试基础间隔 * (2**i))
                        continue
                    break
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
            finally:
                try:
                    signal.alarm(0)
                except Exception:
                    pass

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
        股票最新日期["last_date"] = 股票最新日期["last_date"].map(规范化查询日期)

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
        源端无新增代码: dict[str, str] = {}
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
                        if err == "no data":
                            源端无新增代码[code] = name
                        else:
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
                    else:
                        源端无新增代码[code] = name

                    if 完成 % 50 == 0 or 完成 == 总任务:
                        print(f"  进度: {完成}/{总任务}")

        if 失败列表:
            print(f"\n[重试] {len(失败列表)} 个失败股票串行重试（独立会话）...")
            still_failed: list[tuple[str, str, str]] = []
            task_map = {t["stock_code"]: t for t in tasks}
            for code, name, err_orig in 失败列表:
                t = task_map.get(code)
                if t is None:
                    still_failed.append((code, name, err_orig))
                    continue
                retry_results = _worker_download_batch([t])
                r = retry_results[0] if retry_results else {"ok": False, "error": "retry returned no result"}
                if not r.get("ok"):
                    err = r.get("error", "unknown")
                    if err == "no data":
                        源端无新增代码[code] = name
                        continue
                    still_failed.append((code, name, err))
                    continue

                raw = pd.DataFrame(r["rows"], columns=r["fields"])
                std = BaoStock数据转标准格式(raw, code, name)
                std = std[std["date"] > t["last_date"]]
                std = std[_有效股票代码(std["stock_code"])]
                if std.empty:
                    源端无新增代码[code] = name
                    continue

                新数据列表.append(std)
                print(f"    [重试成功] {code}")
            失败列表 = still_failed

        if not 新数据列表:
            if 失败列表:
                print("\n[错误] 未获得新增日线数据，且仍存在真实失败股票。")
                print(f"失败股票数: {len(失败列表)}")
                print("失败样本（前20）:")
                for code, name, err in 失败列表[:20]:
                    print(f"    {code} {name}: {err}")
                if 源端无新增代码:
                    print(f"源端无新增股票数: {len(源端无新增代码)}")
                raise SystemExit(2)

            print("\n[提示] 未获得新增日线数据，但滞后股票均已判定为源端无更晚数据。")
            if 源端无新增代码:
                print(f"源端无新增股票数: {len(源端无新增代码)}")
                for code, name in list(源端无新增代码.items())[:20]:
                    print(f"    {code} {name}")
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

        当前合并清洗后 = 合并清洗后
        验收结果 = pd.DataFrame()
        for retry_round in range(逐股验收再跑轮数 + 1):
            更新后最新 = (
                当前合并清洗后.groupby("stock_code", as_index=False)["date"]
                .max()
                .rename(columns={"date": "last_date"})
            )
            更新后最新["last_date"] = 更新后最新["last_date"].map(规范化查询日期)
            验收结果 = _打印逐股验收(需要更新, 更新后最新, 最近交易日, 源端无新增代码)
            未完成 = 验收结果[验收结果["status"] == "lagging"].copy()
            if 未完成.empty:
                break
            if retry_round >= 逐股验收再跑轮数:
                break

            print(f"\n[补跑] 发现 {len(未完成)} 只股票未追平，立即重跑第 {retry_round + 1} 轮")
            当前合并清洗后, _, 补跑源端无新增代码 = _针对滞后股票补跑(
                当前合并清洗后,
                未完成.loc[:, ["stock_code", "stock_name"]],
                最近交易日,
            )
            源端无新增代码.update(补跑源端无新增代码)
            原子写入Parquet(当前合并清洗后, 合并数据路径)
            print(f"[OK] 补跑后已重写: {合并数据路径}")

        未完成 = 验收结果[验收结果["status"] == "lagging"].copy()
        if 源端无新增代码:
            print(f"[提示] 以下股票在源端没有更晚日线，按停牌/退市处理跳过: {len(源端无新增代码)}")
            for code, name in list(源端无新增代码.items())[:20]:
                print(f"    {code} {name}")
        if not 未完成.empty:
            print("[错误] 写回后仍有股票未补齐到目标交易日，视为本次更新失败")
            print(f"  未补齐股票数: {len(未完成)}")
            for _, row in 未完成.head(20).iterrows():
                print(f"    {row['stock_code']} {row['stock_name']}: last_date={row['last_date']}")
            raise SystemExit(2)

    finally:
        释放BaoStock锁()


def main() -> None:
    args = parse_args()
    智能更新(dry_run=args.dry_run, limit_stocks=max(0, args.limit_stocks))


if __name__ == "__main__":
    mp.freeze_support()
    main()
