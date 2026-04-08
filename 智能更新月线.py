"""月线智能增量更新（含清洗）。"""

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

from 配置 import 合并数据路径
from 配置月线 import (
    BAOSTOCK_月线_FIELDS,
    并发进程数月线,
    开始日期月线,
    数据月线路径,
    最大重试次数月线,
    复权方式月线,
    输出列名月线,
    频率月线,
    重试基础间隔月线,
)
from 数据清洗规则 import 原子写入Parquet, 打印报告, 清洗月线


BAOSTOCK_LOCK = Path(tempfile.gettempdir()) / "baostock.lock"


def 规范化查询日期(value: object) -> str | None:
    if value is None or pd.isna(value):
        return None
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        text = str(value).strip()
        return text[:10] if len(text) >= 10 else None
    return ts.strftime("%Y-%m-%d")


def 代码转BaoStock格式(stock_code: str) -> str:
    if stock_code.startswith(("6", "9")):
        return f"sh.{stock_code}"
    return f"sz.{stock_code}"


def _有效股票代码(series: pd.Series) -> pd.Series:
    s = series.fillna("").astype(str).str.strip()
    return s.str.fullmatch(r"\d{6}", na=False)


def _最后有效数值(series: pd.Series, default: float = 0.0) -> float:
    values = pd.to_numeric(series, errors="coerce").dropna()
    if values.empty:
        return default
    return float(values.iloc[-1])


def 聚合日线为月线(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=输出列名月线)

    out = df.copy()
    out["stock_code"] = out["stock_code"].astype(str).str.strip()
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out = out[_有效股票代码(out["stock_code"]) & out["date"].notna()].copy()
    if out.empty:
        return pd.DataFrame(columns=输出列名月线)

    for col in ["open", "high", "low", "close", "volume", "amount", "outstanding_share", "turnover"]:
        if col not in out.columns:
            out[col] = 0.0
        out[col] = pd.to_numeric(out[col], errors="coerce")

    if "stock_name" not in out.columns:
        out["stock_name"] = ""
    out["stock_name"] = out["stock_name"].fillna("").astype(str)
    out = out.sort_values(["stock_code", "date"], kind="mergesort")

    monthly = out.groupby("stock_code", as_index=False, sort=False).agg(
        stock_name=("stock_name", "last"),
        date=("date", "max"),
        open=("open", "first"),
        high=("high", "max"),
        low=("low", "min"),
        close=("close", "last"),
        volume=("volume", "sum"),
        amount=("amount", "sum"),
        outstanding_share=("outstanding_share", _最后有效数值),
        turnover=("turnover", "sum"),
    )
    monthly["date"] = monthly["date"].dt.strftime("%Y-%m-%d")
    return monthly[输出列名月线]


def 构建当月临时月线(最新日线日期: str) -> pd.DataFrame:
    latest_ts = pd.Timestamp(最新日线日期)
    month_start = latest_ts.replace(day=1).strftime("%Y-%m-%d")
    daily = pd.read_parquet(
        合并数据路径,
        columns=["stock_code", "stock_name", "date", "open", "high", "low", "close", "volume", "amount", "outstanding_share", "turnover"],
        filters=[("date", ">=", month_start), ("date", "<=", 最新日线日期)],
    )
    if daily.empty:
        return pd.DataFrame(columns=输出列名月线)
    return 聚合日线为月线(daily)


def 替换当月月线(月线df: pd.DataFrame, 当月月线df: pd.DataFrame, 最新日线日期: str) -> pd.DataFrame:
    if 当月月线df.empty:
        return 月线df.copy()

    target_period = pd.Timestamp(最新日线日期).to_period("M")
    if 月线df.empty:
        return 当月月线df.copy()

    base = 月线df.copy()
    base["date"] = pd.to_datetime(base["date"], errors="coerce")
    base = base[base["date"].notna()].copy()
    base = base[base["date"].dt.to_period("M") != target_period].copy()
    base["date"] = base["date"].dt.strftime("%Y-%m-%d")
    return pd.concat([base, 当月月线df], ignore_index=True)


def 检查BaoStock锁() -> bool:
    if BAOSTOCK_LOCK.exists():
        mtime = BAOSTOCK_LOCK.stat().st_mtime
        if time.time() - mtime < 1800:
            return True
        BAOSTOCK_LOCK.unlink(missing_ok=True)
    return False


def 创建BaoStock锁() -> None:
    BAOSTOCK_LOCK.parent.mkdir(parents=True, exist_ok=True)
    BAOSTOCK_LOCK.write_text(f"monthly-update {datetime.datetime.now().isoformat()}", encoding="utf-8")


def 释放BaoStock锁() -> None:
    BAOSTOCK_LOCK.unlink(missing_ok=True)


def 获取最近完整月份交易日() -> str | None:
    today = datetime.date.today()
    this_month_first = today.replace(day=1)
    prev_month_end = this_month_first - datetime.timedelta(days=1)

    start = (prev_month_end - datetime.timedelta(days=45)).strftime("%Y-%m-%d")
    end = prev_month_end.strftime("%Y-%m-%d")

    rs = bs.query_trade_dates(start_date=start, end_date=end)
    trades: list[str] = []
    while rs.next():
        d, is_trade = rs.get_row_data()
        if is_trade == "1" and d <= end:
            trades.append(d)

    if not trades:
        return None
    return sorted(trades)[-1]


def _worker_download(task: dict) -> dict:
    import baostock as bs

    code = task["stock_code"]
    name = task["stock_name"]
    start_date = task["start_date"]
    end_date = task["end_date"]

    try:
        devnull_fd = os.open(os.devnull, os.O_WRONLY)
        old_stderr = os.dup(2)
        os.dup2(devnull_fd, 2)
    except Exception:
        devnull_fd = None
        old_stderr = None

    old_stdout = sys.stdout
    sys.stdout = io.StringIO()
    try:
        lg = bs.login()
    finally:
        sys.stdout = old_stdout

    if lg.error_code != "0":
        if devnull_fd is not None:
            try:
                os.dup2(old_stderr, 2)
                os.close(old_stderr)
                os.close(devnull_fd)
            except Exception:
                pass
        return {"ok": False, "stock_code": code, "stock_name": name, "error": "login failed"}

    bs_code = 代码转BaoStock格式(code)
    rows = []
    fields = None
    err = None

    for i in range(最大重试次数月线):
        try:
            rs = bs.query_history_k_data_plus(
                code=bs_code,
                fields=BAOSTOCK_月线_FIELDS,
                start_date=start_date,
                end_date=end_date,
                frequency=频率月线,
                adjustflag=复权方式月线,
            )
            if rs is None:
                err = "query returned none"
                if i < 最大重试次数月线 - 1:
                    time.sleep(重试基础间隔月线 * (2**i))
                    continue
                break
            if rs.error_code != "0":
                err = rs.error_msg
                if i < 最大重试次数月线 - 1:
                    time.sleep(重试基础间隔月线 * (2**i))
                    continue
                break

            fields = rs.fields
            while rs.next():
                rows.append(rs.get_row_data())
            break
        except Exception as e:
            err = str(e)
            if i < 最大重试次数月线 - 1:
                time.sleep(重试基础间隔月线 * (2**i))

    old_stdout = sys.stdout
    sys.stdout = io.StringIO()
    try:
        bs.logout()
    except Exception:
        pass
    finally:
        sys.stdout = old_stdout

    if devnull_fd is not None:
        try:
            os.dup2(old_stderr, 2)
            os.close(old_stderr)
            os.close(devnull_fd)
        except Exception:
            pass

    if not rows:
        return {"ok": False, "stock_code": code, "stock_name": name, "error": err or "no data"}

    return {
        "ok": True,
        "stock_code": code,
        "stock_name": name,
        "fields": fields,
        "rows": rows,
    }


def 转标准格式(rows: list[list[str]], fields: list[str], stock_code: str, stock_name: str) -> pd.DataFrame:
    df = pd.DataFrame(rows, columns=fields)
    out = pd.DataFrame()
    out["date"] = df["date"]
    out["stock_code"] = stock_code
    out["stock_name"] = stock_name

    for c in ["open", "high", "low", "close", "volume", "amount"]:
        out[c] = pd.to_numeric(df[c], errors="coerce")

    out["outstanding_share"] = 0.0
    out["turnover"] = pd.to_numeric(df["turn"], errors="coerce")
    return out[输出列名月线]


def _获取股票宇宙() -> tuple[pd.DataFrame, str | None]:
    if not 合并数据路径.exists():
        raise FileNotFoundError(f"日线文件不存在: {合并数据路径}")

    df = pd.read_parquet(合并数据路径, columns=["stock_code", "stock_name", "date"])
    df["stock_code"] = df["stock_code"].astype(str).str.strip()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df[_有效股票代码(df["stock_code"]) & df["date"].notna()].copy()
    latest_trade_day = 规范化查询日期(df["date"].max())

    latest = (
        df.sort_values(["stock_code", "date"], kind="mergesort")
        .drop_duplicates(subset=["stock_code"], keep="last")
        .loc[:, ["stock_code", "stock_name"]]
        .reset_index(drop=True)
    )
    return latest, latest_trade_day


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="月线智能增量更新")
    p.add_argument("--dry-run", action="store_true", help="只拉取和校验，不写回")
    p.add_argument("--limit-stocks", type=int, default=0, help="调试：限制更新股票数")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    print("=" * 70)
    print(f"月线智能更新 - {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)
    print(f"dry_run={args.dry_run}, limit_stocks={args.limit_stocks}")

    if 检查BaoStock锁():
        print(f"[警告] 检测到其他BaoStock脚本运行中: {BAOSTOCK_LOCK}")
        return

    创建BaoStock锁()
    try:
        股票宇宙, 最新日线日期 = _获取股票宇宙()

        if 数据月线路径.exists():
            print("\n[1/6] 读取并清洗现有月线...")
            m_raw = pd.read_parquet(数据月线路径)
            m_data, m_report = 清洗月线(m_raw)
            打印报告("现有月线清洗摘要", m_report)
            m_last = m_data.groupby("stock_code", as_index=False)["date"].max().rename(columns={"date": "last_date"})
            m_last["last_date"] = m_last["last_date"].map(规范化查询日期)
        else:
            print("\n[1/6] 现有月线不存在，将执行初始化增量")
            m_data = pd.DataFrame(columns=输出列名月线)
            m_last = pd.DataFrame(columns=["stock_code", "last_date"])

        print("\n[2/6] 获取最近完整月份交易日...")
        lg = bs.login()
        if lg.error_code != "0":
            print(f"[错误] BaoStock登录失败: {lg.error_msg}")
            return

        target_month_day = 获取最近完整月份交易日()
        bs.logout()

        if not target_month_day:
            print("[错误] 未获取到最近完整月线日期")
            return

        print(f"  目标月线日期: {target_month_day}")
        print(f"  最新日线日期: {最新日线日期 or '-'}")

        print("\n[3/6] 构建更新任务...")
        merged = 股票宇宙.merge(m_last, on="stock_code", how="left")
        need = merged[merged["last_date"].isna() | (merged["last_date"] < target_month_day)].copy()
        need = need.sort_values(["last_date", "stock_code"], na_position="first").reset_index(drop=True)

        if args.limit_stocks > 0:
            need = need.head(args.limit_stocks).copy()
            print(f"  [调试] 限制更新股票数: {len(need)}")

        print(f"  股票宇宙: {len(股票宇宙)}")
        print(f"  需更新: {len(need)}")

        新增列表: list[pd.DataFrame] = []
        失败列表: list[tuple[str, str, str]] = []
        源端无新增代码: dict[str, str] = {}
        if need.empty:
            print("\n[4/6] 完整月份月线已是最新，跳过BaoStock月线下载")
        else:
            print(f"\n[4/6] 并发下载（{并发进程数月线} 进程）...")
            tasks = []
            for _, r in need.iterrows():
                last_date = 规范化查询日期(r["last_date"])
                if last_date is None:
                    start_date = 开始日期月线
                elif last_date[:7] == target_month_day[:7]:
                    start_date = f"{target_month_day[:7]}-01"
                else:
                    start_date = last_date
                tasks.append(
                    {
                        "stock_code": r["stock_code"],
                        "stock_name": r["stock_name"],
                        "start_date": start_date,
                        "end_date": target_month_day,
                        "last_date": last_date,
                    }
                )

            done = 0
            with ProcessPoolExecutor(max_workers=并发进程数月线) as ex:
                fut_map = {ex.submit(_worker_download, t): t for t in tasks}
                for fut in as_completed(fut_map):
                    t = fut_map[fut]
                    done += 1

                    try:
                        r = fut.result()
                    except Exception as e:
                        失败列表.append((t["stock_code"], t["stock_name"], str(e)))
                        if done % 50 == 0 or done == len(tasks):
                            print(f"  进度: {done}/{len(tasks)}")
                        continue

                    if not r.get("ok"):
                        err = r.get("error", "unknown")
                        if err == "no data":
                            源端无新增代码[t["stock_code"]] = t["stock_name"]
                        else:
                            失败列表.append((t["stock_code"], t["stock_name"], err))
                        if done % 50 == 0 or done == len(tasks):
                            print(f"  进度: {done}/{len(tasks)}")
                        continue

                    df = 转标准格式(r["rows"], r["fields"], r["stock_code"], r["stock_name"])
                    last_d = t.get("last_date")
                    if last_d:
                        df = df[df["date"] > last_d]
                    if not df.empty:
                        新增列表.append(df)
                    else:
                        源端无新增代码[t["stock_code"]] = t["stock_name"]

                    if done % 50 == 0 or done == len(tasks):
                        print(f"  进度: {done}/{len(tasks)}")

            if 失败列表:
                print(f"\n[重试] {len(失败列表)} 个失败股票串行重试（独立会话）...")
                still_failed: list[tuple[str, str, str]] = []
                task_map = {t["stock_code"]: t for t in tasks}
                for code, name, err_orig in 失败列表:
                    t = task_map.get(code)
                    if t is None:
                        still_failed.append((code, name, err_orig))
                        continue
                    r = _worker_download(t)
                    if not r.get("ok"):
                        err = r.get("error", "unknown")
                        if err == "no data":
                            源端无新增代码[code] = name
                            continue
                        still_failed.append((code, name, err))
                        continue

                    df = 转标准格式(r["rows"], r["fields"], r["stock_code"], r["stock_name"])
                    last_d = t.get("last_date")
                    if last_d:
                        df = df[df["date"] > last_d]
                    if df.empty:
                        源端无新增代码[code] = name
                        continue

                    新增列表.append(df)
                    print(f"    [重试成功] {code}")
                失败列表 = still_failed

        有效待补股票 = need
        if 源端无新增代码:
            有效待补股票 = need[~need["stock_code"].isin(源端无新增代码)].copy()

        if not 有效待补股票.empty and not 新增列表:
            print("\n[错误] 未下载到新增月线数据，但仍有股票落后于目标月线日期。")
            if 失败列表:
                print(f"失败股票数: {len(失败列表)}")
                print("失败样本（前20）:")
                for code, name, err in 失败列表[:20]:
                    print(f"    {code} {name}: {err}")
            if 源端无新增代码:
                print(f"源端无新增股票数: {len(源端无新增代码)}")
            raise SystemExit(2)

        print("\n[5/6] 合并并清洗...")
        新增合并 = pd.concat(新增列表, ignore_index=True) if 新增列表 else pd.DataFrame(columns=输出列名月线)
        merged_df = pd.concat([m_data, 新增合并], ignore_index=True)
        当月临时月线 = pd.DataFrame(columns=输出列名月线)
        if 最新日线日期 and 最新日线日期 > target_month_day:
            当月临时月线 = 构建当月临时月线(最新日线日期)
            if 当月临时月线.empty:
                print(f"  [警告] 未生成当月临时月线（截止 {最新日线日期}）")
            else:
                merged_df = 替换当月月线(merged_df, 当月临时月线, 最新日线日期)
                print(f"  当月临时月线行数: {len(当月临时月线):,}（截止 {最新日线日期}）")
        cleaned, report = 清洗月线(merged_df)
        打印报告("合并后月线清洗摘要", report)

        print("\n[6/6] 写回文件...")
        print(f"  完整月份新增行数: {len(新增合并):,}")
        print(f"  当月临时月线行数: {len(当月临时月线):,}")
        print(f"  更新后总行数: {len(cleaned):,}")
        print(f"  失败股票数: {len(失败列表)}")

        if 失败列表:
            print("  失败样本（前20）:")
            for code, name, err in 失败列表[:20]:
                print(f"    {code} {name}: {err}")

        if args.dry_run:
            print("[DRY-RUN] 不写回月线文件")
            return

        原子写入Parquet(cleaned, 数据月线路径)
        print(f"[OK] 已写回: {数据月线路径}")

        if not need.empty:
            cleaned_target = cleaned.copy()
            cleaned_target["date"] = pd.to_datetime(cleaned_target["date"], errors="coerce")
            target_period = pd.Timestamp(target_month_day).to_period("M")
            target_rows = cleaned_target[cleaned_target["date"].dt.to_period("M") == target_period].copy()
            target_rows["target_date"] = target_rows["date"].map(规范化查询日期)
            target_latest = (
                target_rows.groupby("stock_code", as_index=False)["target_date"]
                .max()
            )
            未完成 = 有效待补股票.loc[:, ["stock_code", "stock_name"]].merge(target_latest, on="stock_code", how="left")
            未完成 = 未完成[未完成["target_date"] != target_month_day].copy()
            if 源端无新增代码:
                print(f"[提示] 以下股票在源端没有更晚完整月线，按停牌/退市处理跳过: {len(源端无新增代码)}")
                for code, name in list(源端无新增代码.items())[:20]:
                    print(f"    {code} {name}")
            if not 未完成.empty:
                print("[错误] 写回后仍有股票未补齐到目标月线日期，视为本次更新失败")
                print(f"  未补齐股票数: {len(未完成)}")
                for _, row in 未完成.head(20).iterrows():
                    print(f"    {row['stock_code']} {row['stock_name']}: target_date={row['target_date']}")
                raise SystemExit(2)

        if not 当月临时月线.empty:
            更新后最新 = (
                cleaned.groupby("stock_code", as_index=False)["date"]
                .max()
                .rename(columns={"date": "last_date"})
            )
            更新后最新["last_date"] = 更新后最新["last_date"].map(规范化查询日期)
            期望当月最新 = 当月临时月线.loc[:, ["stock_code", "date"]].rename(columns={"date": "expected_date"})
            期望当月最新["expected_date"] = 期望当月最新["expected_date"].map(规范化查询日期)
            临时未完成 = 期望当月最新.merge(更新后最新, on="stock_code", how="left")
            临时未完成 = 临时未完成[临时未完成["last_date"] != 临时未完成["expected_date"]].copy()
            if not 临时未完成.empty:
                print("[错误] 写回后当月临时月线未更新到最新日线日期，视为本次更新失败")
                print(f"  未补齐股票数: {len(临时未完成)}")
                for _, row in 临时未完成.head(20).iterrows():
                    print(f"    {row['stock_code']}: last_date={row['last_date']}, expected={row['expected_date']}")
                raise SystemExit(2)

    finally:
        释放BaoStock锁()


if __name__ == "__main__":
    mp.freeze_support()
    main()
