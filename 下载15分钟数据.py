"""全市场15分钟K线数据下载脚本

v4 - 使用多进程并发：
- BaoStock 不是线程安全的（共享socket），多线程会导致数据返回为空
- 改用 multiprocessing：每个子进程独立 bs.login()，互不干扰
- 每只股票60秒超时保护
- 实时逐只显示下载进度
- 3个工作进程并发
"""

import sys
import os
import io
import datetime
import time
import json
import multiprocessing as mp
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed, TimeoutError
import pandas as pd

from 配置 import 合并数据路径
from 配置15分钟 import (
    临时文件目录, 数据15分钟路径, 进度文件路径,
    BAOSTOCK_15MIN_FIELDS, 复权方式15分钟, 频率15分钟,
    开始日期, 结束日期, 批量大小,
    最大重试次数, 重试基础间隔, 输出列名15分钟,
)

# 并发进程数
并发数 = 3

# 单只股票最大等待秒数
单股超时秒 = 90


# ==================== 输出工具 ====================

def log(msg: str):
    """立即输出一行日志到stdout"""
    try:
        print(msg, flush=True)
    except UnicodeEncodeError:
        print(msg.encode("gbk", errors="replace").decode("gbk"), flush=True)


# ==================== 工具函数 ====================

def 代码转BaoStock格式(stock_code: str) -> str:
    if stock_code.startswith(("6", "9")):
        return f"sh.{stock_code}"
    return f"sz.{stock_code}"


def 解析时间戳(时间戳: str) -> str:
    try:
        if len(时间戳) >= 14:
            return f"{时间戳[8:10]}:{时间戳[10:12]}:{时间戳[12:14]}"
        return ""
    except Exception:
        return ""


# ==================== 股票列表 ====================

def 获取股票列表() -> list[dict]:
    if not 合并数据路径.exists():
        log(f"[ERROR] daily data not found: {合并数据路径}")
        sys.exit(1)
    日线数据 = pd.read_parquet(合并数据路径)
    股票列表 = 日线数据[["stock_code", "stock_name"]].drop_duplicates()
    return 股票列表.to_dict("records")


# ==================== 子进程下载函数 ====================

def _worker_download(stock_info: dict) -> dict:
    """
    在子进程中运行：独立登录BaoStock，下载一只股票，返回结果dict。
    返回格式：{"code": str, "rows": list[list] | None, "fields": list | None, "name": str}
    注意：不能返回DataFrame（跨进程序列化太慢），返回原始行数据。
    """
    import baostock as bs

    代码 = stock_info["stock_code"]
    名称 = stock_info["stock_name"]
    bs代码 = 代码转BaoStock格式(代码)
    结束 = 结束日期 if 结束日期 else datetime.date.today().strftime("%Y-%m-%d")

    # 抑制BaoStock的stderr输出（OS级）
    try:
        devnull_fd = os.open(os.devnull, os.O_WRONLY)
        old_stderr = os.dup(2)
        os.dup2(devnull_fd, 2)
    except Exception:
        devnull_fd = None
        old_stderr = None

    # 抑制BaoStock login的stdout输出
    old_stdout = sys.stdout
    sys.stdout = io.StringIO()
    try:
        lg = bs.login()
    finally:
        sys.stdout = old_stdout

    if lg.error_code != "0":
        if devnull_fd is not None:
            os.dup2(old_stderr, 2)
            os.close(old_stderr)
            os.close(devnull_fd)
        return {"code": 代码, "rows": None, "fields": None, "name": 名称, "error": "login failed"}

    result_rows = None
    result_fields = None
    error_msg = None

    for 尝试 in range(最大重试次数):
        try:
            rs = bs.query_history_k_data_plus(
                code=bs代码,
                fields=BAOSTOCK_15MIN_FIELDS,
                start_date=开始日期,
                end_date=结束,
                frequency=频率15分钟,
                adjustflag=复权方式15分钟,
            )

            if rs.error_code != "0":
                error_msg = rs.error_msg
                if 尝试 < 最大重试次数 - 1:
                    time.sleep(重试基础间隔 * (2 ** 尝试))
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
            if 尝试 < 最大重试次数 - 1:
                time.sleep(重试基础间隔 * (2 ** 尝试))

    # logout（静默）
    old_stdout2 = sys.stdout
    sys.stdout = io.StringIO()
    try:
        bs.logout()
    except Exception:
        pass
    finally:
        sys.stdout = old_stdout2

    # 恢复stderr
    if devnull_fd is not None:
        try:
            os.dup2(old_stderr, 2)
            os.close(old_stderr)
            os.close(devnull_fd)
        except Exception:
            pass

    return {
        "code": 代码,
        "rows": result_rows,
        "fields": result_fields,
        "name": 名称,
        "error": error_msg,
    }


def BaoStock数据转15分钟格式(rows, fields, stock_code, stock_name):
    """将原始行数据转换为标准DataFrame"""
    df = pd.DataFrame(rows, columns=fields)
    结果 = pd.DataFrame()
    结果["date"] = df["date"]
    结果["stock_code"] = stock_code
    结果["stock_name"] = stock_name
    结果["time"] = df["time"].apply(解析时间戳)
    for col in ["open", "high", "low", "close", "volume", "amount"]:
        结果[col] = pd.to_numeric(df[col], errors="coerce")
    return 结果[输出列名15分钟]


# ==================== 并发下载（多进程+超时） ====================

def 并发下载一批(股票列表: list[dict], 批次标签: str) -> tuple[list, list]:
    """使用进程池并发下载一批股票"""
    所有数据 = []
    失败列表 = []
    总数 = len(股票列表)
    已完成 = 0

    with ProcessPoolExecutor(max_workers=并发数) as executor:
        future_to_stock = {
            executor.submit(_worker_download, s): s for s in 股票列表
        }

        for future in as_completed(future_to_stock):
            股票 = future_to_stock[future]
            已完成 += 1
            代码 = 股票["stock_code"]

            try:
                result = future.result(timeout=单股超时秒)

                if result["rows"] is not None:
                    df = BaoStock数据转15分钟格式(
                        result["rows"], result["fields"],
                        result["code"], result["name"]
                    )
                    所有数据.append(df)
                    log(f"  {批次标签} [{已完成}/{总数}] {代码} OK ({len(df)} rows)")
                else:
                    失败列表.append(股票)
                    log(f"  {批次标签} [{已完成}/{总数}] {代码} -- no data")
            except TimeoutError:
                失败列表.append(股票)
                log(f"  {批次标签} [{已完成}/{总数}] {代码} TIMEOUT (>{单股超时秒}s)")
                future.cancel()
            except Exception as e:
                失败列表.append(股票)
                err = str(e)[:80]
                log(f"  {批次标签} [{已完成}/{总数}] {代码} ERROR: {err}")

    return 所有数据, 失败列表


# ==================== 断点续传 ====================

def 保存进度(已完成列表: list[str]):
    进度文件路径.parent.mkdir(parents=True, exist_ok=True)
    with open(进度文件路径, "w", encoding="utf-8") as f:
        json.dump({
            "已完成股票": 已完成列表,
            "完成数量": len(已完成列表),
            "更新时间": datetime.datetime.now().isoformat(),
        }, f, ensure_ascii=False, indent=2)


def 读取进度() -> set[str]:
    if not 进度文件路径.exists():
        return set()
    try:
        with open(进度文件路径, "r", encoding="utf-8") as f:
            return set(json.load(f).get("已完成股票", []))
    except Exception:
        return set()


# ==================== 分批保存 ====================

def 分批保存(数据列表: list[pd.DataFrame], 批次号: int):
    if not 数据列表:
        return
    临时文件目录.mkdir(parents=True, exist_ok=True)
    合并 = pd.concat(数据列表, ignore_index=True)
    路径 = 临时文件目录 / f"batch_{批次号:04d}.parquet"
    合并.to_parquet(路径, engine="pyarrow", compression="snappy", index=False)
    log(f"  >> saved {路径.name} ({len(合并):,} rows)")


# ==================== 最终合并 ====================

def 合并所有临时文件() -> pd.DataFrame:
    文件列表 = sorted(临时文件目录.glob("batch_*.parquet"))
    if not 文件列表:
        log("[ERROR] no batch files found")
        sys.exit(1)

    log(f"  found {len(文件列表)} batch files")
    所有 = [pd.read_parquet(f) for f in 文件列表]
    合并 = pd.concat(所有, ignore_index=True)
    log(f"  total rows before dedup: {len(合并):,}")

    原始 = len(合并)
    合并 = 合并.drop_duplicates(subset=["stock_code", "date", "time"], keep="last")
    if len(合并) < 原始:
        log(f"  dedup removed: {原始 - len(合并):,}")

    合并 = 合并.sort_values(["stock_code", "date", "time"]).reset_index(drop=True)

    数据15分钟路径.parent.mkdir(parents=True, exist_ok=True)
    合并.to_parquet(数据15分钟路径, engine="pyarrow", compression="snappy", index=False)
    log(f"  saved: {数据15分钟路径}")
    return 合并


# ==================== 数据质量检查 ====================

def 数据质量检查(df: pd.DataFrame):
    log("\n" + "="*70)
    log("DATA QUALITY CHECK")
    log("="*70)

    log(f"\n[Basic]")
    log(f"  rows:   {len(df):,}")
    log(f"  stocks: {df['stock_code'].nunique():,}")
    log(f"  range:  {df['date'].min()} ~ {df['date'].max()}")

    log(f"\n[Missing]")
    缺失 = df.isnull().sum()
    缺失 = 缺失[缺失 > 0]
    if len(缺失) > 0:
        for col, n in 缺失.items():
            log(f"  {col}: {n:,} ({n/len(df)*100:.2f}%)")
    else:
        log("  [OK] no missing values")

    log(f"\n[Price range]")
    for col in ["open", "high", "low", "close"]:
        log(f"  {col}: {df[col].min():.2f} ~ {df[col].max():.2f}")

    样本 = df['close'].dropna().head(1000).astype(str)
    小数位 = [len(p.split('.')[1]) if '.' in p else 0 for p in 样本]
    平均 = sum(小数位) / len(小数位) if 小数位 else 0
    log(f"\n[Adjust flag]")
    log(f"  avg decimals: {平均:.1f}")
    log(f"  {'[OK] unadjusted' if 平均 < 3 else '[WARN] may be adjusted'}")

    每股 = df.groupby("stock_code").size()
    log(f"\n[Completeness]")
    log(f"  avg rows/stock: {每股.mean():.0f}")
    log(f"  min: {每股.min()}, max: {每股.max()}")

    MB = 数据15分钟路径.stat().st_size / (1024 * 1024)
    log(f"\n[Storage]")
    log(f"  file size: {MB:.1f} MB")
    log(f"  path: {数据15分钟路径}")


# ==================== 主函数 ====================

def main():
    log("="*70)
    log("15min K-line Download (multiprocess)")
    log("="*70)
    log(f"range:    {开始日期} ~ today")
    log(f"adjust:   unadjusted")
    log(f"workers:  {并发数} processes")
    log(f"timeout:  {单股超时秒}s per stock")
    log(f"batch:    {批量大小}")
    log("="*70)

    # [1/6]
    log("\n[1/6] Loading stock list...")
    股票列表 = 获取股票列表()
    总数 = len(股票列表)
    log(f"  total: {总数:,}")

    # [2/6]
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
        if 临时文件目录.exists() and list(临时文件目录.glob("batch_*.parquet")):
            log("\n[5/6] Merging...")
            最终 = 合并所有临时文件()
            log("\n[6/6] Quality check...")
            数据质量检查(最终)
        return

    # [3/6] - 主进程不需要login，子进程各自独立login
    log("\n[3/6] BaoStock connection: each worker connects independently")

    # [4/6]
    log(f"\n[4/6] Downloading ({并发数} workers, {单股超时秒}s timeout)...")
    log("="*70)
    开始时间 = time.time()

    批次号 = len(list(临时文件目录.glob("batch_*.parquet"))) if 临时文件目录.exists() else 0
    全部已完成 = list(已完成集合)
    全部失败 = []
    待下载总数 = len(股票列表)

    for i in range(0, 待下载总数, 批量大小):
        批次股票 = 股票列表[i:i+批量大小]
        当前批次 = i // 批量大小 + 1
        总批次 = (待下载总数 + 批量大小 - 1) // 批量大小

        log(f"\n{'='*70}")
        log(f"BATCH {当前批次}/{总批次} ({len(批次股票)} stocks)")
        log(f"{'='*70}")
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

        # 汇总
        已耗时 = time.time() - 开始时间
        完成数 = len(全部已完成) - len(已完成集合)
        进度 = 完成数 / 待下载总数 * 100

        log(f"\n  BATCH SUMMARY: ok={len(数据列表)} fail={len(失败列表)} time={批次耗时:.0f}s")

        if 完成数 > 0:
            速度 = 已耗时 / 完成数
            剩余 = (待下载总数 - 完成数) * 速度 / 60
            log(f"  TOTAL: {完成数}/{待下载总数} ({进度:.1f}%) | "
                f"elapsed={已耗时/60:.1f}min | remaining~{剩余:.0f}min")

    耗时 = time.time() - 开始时间

    log(f"\n{'='*70}")
    log(f"DOWNLOAD COMPLETE")
    log(f"{'='*70}")
    log(f"  time:    {耗时/60:.1f} min ({耗时/3600:.2f} h)")
    log(f"  success: {len(全部已完成) - len(已完成集合)}")
    log(f"  failed:  {len(全部失败)}")

    if 全部失败:
        log(f"  failed samples (first 10):")
        for s in 全部失败[:10]:
            log(f"    {s['stock_code']}")

    # [5/6]
    log("\n[5/6] Merging batch files...")
    最终 = 合并所有临时文件()

    # [6/6]
    log("\n[6/6] Quality check...")
    数据质量检查(最终)

    log(f"\n{'='*70}")
    log(f"ALL DONE!")
    log(f"  rows: {len(最终):,}")
    log(f"  file: {数据15分钟路径}")
    log(f"  size: {数据15分钟路径.stat().st_size/(1024*1024):.1f} MB")
    log(f"{'='*70}")


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
