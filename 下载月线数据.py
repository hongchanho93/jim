"""全市场月线K线数据下载脚本

特点：
- 全市场（基于现有日线Parquet中的股票列表）
- 月线频率（frequency="m"）
- 不复权价格（adjustflag="3"）
- 包含月换手率（turnover）
- 输出列与日线格式一致
"""

import datetime
import io
import json
import multiprocessing as mp
import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor, TimeoutError, as_completed

import pandas as pd

from 配置 import 合并数据路径
from 配置月线 import (
    BAOSTOCK_月线_FIELDS,
    临时文件目录月线,
    单股超时秒月线,
    并发进程数月线,
    开始日期月线,
    批量大小月线,
    数据月线路径,
    最大重试次数月线,
    结束日期月线,
    输出列名月线,
    复权方式月线,
    进度文件路径月线,
    频率月线,
    重试基础间隔月线,
)


def log(msg: str):
    """立即输出一行日志到stdout。"""
    try:
        print(msg, flush=True)
    except UnicodeEncodeError:
        print(msg.encode("gbk", errors="replace").decode("gbk"), flush=True)


def 代码转BaoStock格式(stock_code: str) -> str:
    if stock_code.startswith(("6", "9")):
        return f"sh.{stock_code}"
    return f"sz.{stock_code}"


def 获取股票列表() -> list[dict]:
    if not 合并数据路径.exists():
        log(f"[ERROR] daily data not found: {合并数据路径}")
        sys.exit(1)

    日线数据 = pd.read_parquet(合并数据路径, columns=["stock_code", "stock_name"])
    日线数据["stock_code"] = 日线数据["stock_code"].astype(str).str.strip()
    日线数据["stock_name"] = 日线数据["stock_name"].fillna("").astype(str).str.strip()
    日线数据 = 日线数据[日线数据["stock_code"].str.fullmatch(r"\d{6}", na=False)]

    股票列表 = (
        日线数据.drop_duplicates(subset=["stock_code"], keep="last")
        .sort_values("stock_code")
        .reset_index(drop=True)
    )
    return 股票列表.to_dict("records")


def _worker_download(stock_info: dict) -> dict:
    """子进程下载单只股票月线数据。"""
    import baostock as bs

    代码 = stock_info["stock_code"]
    名称 = stock_info["stock_name"]
    bs代码 = 代码转BaoStock格式(代码)
    结束 = 结束日期月线 if 结束日期月线 else datetime.date.today().strftime("%Y-%m-%d")

    # 抑制BaoStock输出
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
        return {"code": 代码, "name": 名称, "rows": None, "fields": None, "error": "login failed"}

    result_rows = None
    result_fields = None
    error_msg = None

    for 尝试 in range(最大重试次数月线):
        try:
            rs = bs.query_history_k_data_plus(
                code=bs代码,
                fields=BAOSTOCK_月线_FIELDS,
                start_date=开始日期月线,
                end_date=结束,
                frequency=频率月线,
                adjustflag=复权方式月线,
            )
            if rs.error_code != "0":
                error_msg = rs.error_msg
                if 尝试 < 最大重试次数月线 - 1:
                    time.sleep(重试基础间隔月线 * (2 ** 尝试))
                    continue
                break

            rows = []
            while rs.next():
                rows.append(rs.get_row_data())

            if rows:
                result_rows = rows
                result_fields = rs.fields
            break
        except Exception as e:
            error_msg = str(e)
            if 尝试 < 最大重试次数月线 - 1:
                time.sleep(重试基础间隔月线 * (2 ** 尝试))

    old_stdout2 = sys.stdout
    sys.stdout = io.StringIO()
    try:
        bs.logout()
    except Exception:
        pass
    finally:
        sys.stdout = old_stdout2

    if devnull_fd is not None:
        try:
            os.dup2(old_stderr, 2)
            os.close(old_stderr)
            os.close(devnull_fd)
        except Exception:
            pass

    return {
        "code": 代码,
        "name": 名称,
        "rows": result_rows,
        "fields": result_fields,
        "error": error_msg,
    }


def BaoStock数据转月线格式(rows, fields, stock_code: str, stock_name: str) -> pd.DataFrame:
    """转换BaoStock月线数据为项目标准格式。"""
    df = pd.DataFrame(rows, columns=fields)
    结果 = pd.DataFrame()
    结果["date"] = df["date"]
    结果["stock_code"] = stock_code
    结果["stock_name"] = stock_name

    for col in ["open", "high", "low", "close", "volume", "amount"]:
        结果[col] = pd.to_numeric(df[col], errors="coerce")

    turn = pd.to_numeric(df["turn"], errors="coerce")
    结果["turnover"] = turn

    结果["outstanding_share"] = 0.0
    有效 = turn.notna() & (turn > 0)
    结果.loc[有效, "outstanding_share"] = 结果.loc[有效, "volume"] / turn[有效] * 100

    return 结果[输出列名月线]


def 并发下载一批(股票列表: list[dict], 批次标签: str) -> tuple[list[pd.DataFrame], list[dict]]:
    """使用进程池并发下载一批股票。"""
    所有数据 = []
    失败列表 = []
    总数 = len(股票列表)
    已完成 = 0

    with ProcessPoolExecutor(max_workers=并发进程数月线) as executor:
        future_to_stock = {executor.submit(_worker_download, s): s for s in 股票列表}

        for future in as_completed(future_to_stock):
            股票 = future_to_stock[future]
            已完成 += 1
            代码 = 股票["stock_code"]

            try:
                result = future.result(timeout=单股超时秒月线)
                if result["rows"] is not None:
                    df = BaoStock数据转月线格式(
                        result["rows"], result["fields"], result["code"], result["name"]
                    )
                    所有数据.append(df)
                    log(f"  {批次标签} [{已完成}/{总数}] {代码} OK ({len(df)} rows)")
                else:
                    失败列表.append(股票)
                    log(f"  {批次标签} [{已完成}/{总数}] {代码} -- no data")
            except TimeoutError:
                失败列表.append(股票)
                log(f"  {批次标签} [{已完成}/{总数}] {代码} TIMEOUT (>{单股超时秒月线}s)")
                future.cancel()
            except Exception as e:
                失败列表.append(股票)
                log(f"  {批次标签} [{已完成}/{总数}] {代码} ERROR: {str(e)[:80]}")

    return 所有数据, 失败列表


def 保存进度(已完成列表: list[str]):
    进度文件路径月线.parent.mkdir(parents=True, exist_ok=True)
    with open(进度文件路径月线, "w", encoding="utf-8") as f:
        json.dump(
            {
                "已完成股票": 已完成列表,
                "完成数量": len(已完成列表),
                "更新时间": datetime.datetime.now().isoformat(),
            },
            f,
            ensure_ascii=False,
            indent=2,
        )


def 读取进度() -> set[str]:
    if not 进度文件路径月线.exists():
        return set()
    try:
        with open(进度文件路径月线, "r", encoding="utf-8") as f:
            return set(json.load(f).get("已完成股票", []))
    except Exception:
        return set()


def 分批保存(数据列表: list[pd.DataFrame], 批次号: int):
    if not 数据列表:
        return
    临时文件目录月线.mkdir(parents=True, exist_ok=True)
    合并 = pd.concat(数据列表, ignore_index=True)
    路径 = 临时文件目录月线 / f"batch_{批次号:04d}.parquet"
    合并.to_parquet(路径, engine="pyarrow", compression="snappy", index=False)
    log(f"  >> saved {路径.name} ({len(合并):,} rows)")


def 合并所有临时文件() -> pd.DataFrame:
    文件列表 = sorted(临时文件目录月线.glob("batch_*.parquet"))
    if not 文件列表:
        log("[ERROR] no batch files found")
        sys.exit(1)

    log(f"  found {len(文件列表)} batch files")
    所有 = [pd.read_parquet(f) for f in 文件列表]
    合并 = pd.concat(所有, ignore_index=True)
    log(f"  total rows before dedup: {len(合并):,}")

    原始行数 = len(合并)
    合并 = 合并.drop_duplicates(subset=["stock_code", "date"], keep="last")
    if len(合并) < 原始行数:
        log(f"  dedup removed: {原始行数 - len(合并):,}")

    合并 = 合并.sort_values(["stock_code", "date"]).reset_index(drop=True)
    数据月线路径.parent.mkdir(parents=True, exist_ok=True)
    合并.to_parquet(数据月线路径, engine="pyarrow", compression="snappy", index=False)
    log(f"  saved: {数据月线路径}")
    return 合并


def 数据质量检查(df: pd.DataFrame):
    log("\n" + "=" * 70)
    log("DATA QUALITY CHECK")
    log("=" * 70)
    log(f"  rows:   {len(df):,}")
    log(f"  stocks: {df['stock_code'].nunique():,}")
    log(f"  range:  {df['date'].min()} ~ {df['date'].max()}")
    log(f"  turnover missing: {int(df['turnover'].isna().sum()):,}")
    log(f"  file size: {数据月线路径.stat().st_size / (1024 * 1024):.1f} MB")


def main():
    log("=" * 70)
    log("Monthly K-line Download (multiprocess)")
    log("=" * 70)
    log(f"range:    {开始日期月线} ~ today")
    log("adjust:   unadjusted (adjustflag=3)")
    log("freq:     monthly (m)")
    log(f"workers:  {并发进程数月线} processes")
    log(f"batch:    {批量大小月线}")
    log("=" * 70)

    log("\n[1/6] Loading stock list...")
    股票列表 = 获取股票列表()
    总数 = len(股票列表)
    log(f"  total: {总数:,}")

    log("\n[2/6] Checking resume progress...")
    已完成集合 = 读取进度()
    if 已完成集合:
        股票列表 = [s for s in 股票列表 if s["stock_code"] not in 已完成集合]
        log(f"  done:      {len(已完成集合):,}")
        log(f"  remaining: {len(股票列表):,}")
    else:
        log(f"  fresh start: {len(股票列表):,}")

    if not 股票列表:
        log("\n[DONE] All stocks already downloaded!")
        if 临时文件目录月线.exists() and list(临时文件目录月线.glob("batch_*.parquet")):
            log("\n[5/6] Merging...")
            最终 = 合并所有临时文件()
            log("\n[6/6] Quality check...")
            数据质量检查(最终)
        return

    log("\n[3/6] BaoStock connection: each worker connects independently")
    log(f"\n[4/6] Downloading ({并发进程数月线} workers)...")
    log("=" * 70)

    开始时间 = time.time()
    批次号 = (
        len(list(临时文件目录月线.glob("batch_*.parquet"))) if 临时文件目录月线.exists() else 0
    )
    全部已完成 = list(已完成集合)
    全部失败 = []
    待下载总数 = len(股票列表)

    for i in range(0, 待下载总数, 批量大小月线):
        批次股票 = 股票列表[i : i + 批量大小月线]
        当前批次 = i // 批量大小月线 + 1
        总批次 = (待下载总数 + 批量大小月线 - 1) // 批量大小月线

        log(f"\n{'=' * 70}")
        log(f"BATCH {当前批次}/{总批次} ({len(批次股票)} stocks)")
        log(f"{'=' * 70}")
        批次开始 = time.time()

        数据列表, 失败列表 = 并发下载一批(批次股票, f"B{当前批次}")
        批次耗时 = time.time() - 批次开始

        if 数据列表:
            分批保存(数据列表, 批次号)
            批次号 += 1

        成功代码 = [s["stock_code"] for s in 批次股票 if s not in 失败列表]
        全部已完成.extend(成功代码)
        全部失败.extend(失败列表)
        保存进度(全部已完成)

        已耗时 = time.time() - 开始时间
        完成数 = len(全部已完成) - len(已完成集合)
        进度 = 完成数 / 待下载总数 * 100

        log(f"\n  BATCH SUMMARY: ok={len(数据列表)} fail={len(失败列表)} time={批次耗时:.0f}s")
        if 完成数 > 0:
            单股耗时 = 已耗时 / 完成数
            剩余分钟 = (待下载总数 - 完成数) * 单股耗时 / 60
            log(
                f"  TOTAL: {完成数}/{待下载总数} ({进度:.1f}%) | "
                f"elapsed={已耗时/60:.1f}min | remaining~{剩余分钟:.0f}min"
            )

    总耗时 = time.time() - 开始时间
    log(f"\n{'=' * 70}")
    log("DOWNLOAD COMPLETE")
    log(f"{'=' * 70}")
    log(f"  time:    {总耗时/60:.1f} min ({总耗时/3600:.2f} h)")
    log(f"  success: {len(全部已完成) - len(已完成集合)}")
    log(f"  failed:  {len(全部失败)}")

    if 全部失败:
        log("  failed samples (first 10):")
        for s in 全部失败[:10]:
            log(f"    {s['stock_code']}")

    log("\n[5/6] Merging batch files...")
    最终 = 合并所有临时文件()

    log("\n[6/6] Quality check...")
    数据质量检查(最终)

    log(f"\n{'=' * 70}")
    log("ALL DONE!")
    log(f"  rows: {len(最终):,}")
    log(f"  file: {数据月线路径}")
    log(f"  size: {数据月线路径.stat().st_size / (1024 * 1024):.1f} MB")
    log(f"{'=' * 70}")


if __name__ == "__main__":
    mp.freeze_support()  # Windows multiprocessing 需要
    try:
        main()
    except KeyboardInterrupt:
        log("\n\n[INTERRUPTED] Ctrl+C")
        log("saved progress; rerun to resume")
        sys.exit(0)
    except Exception as e:
        log(f"\n[ERROR] {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)

