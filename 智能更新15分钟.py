"""智能增量更新全市场15分钟K线数据（多进程并发）"""

import sys
import os
import io
import datetime
import time
import tempfile
import multiprocessing as mp
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
import pandas as pd
import baostock as bs

from 配置15分钟 import (
    数据15分钟路径,
    BAOSTOCK_15MIN_FIELDS,
    复权方式15分钟,
    频率15分钟,
    输出列名15分钟,
    最大重试次数,
    重试基础间隔,
)

# 15分钟增量更新并发进程数
并发进程数 = 3

# BaoStock互斥锁（与其他BaoStock脚本共享）
BAOSTOCK_LOCK = Path(tempfile.gettempdir()) / "baostock.lock"


def log(msg: str):
    try:
        print(msg, flush=True)
    except UnicodeEncodeError:
        print(msg.encode("gbk", errors="replace").decode("gbk"), flush=True)


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


def 检查BaoStock锁() -> bool:
    if BAOSTOCK_LOCK.exists():
        锁文件时间 = BAOSTOCK_LOCK.stat().st_mtime
        if time.time() - 锁文件时间 < 1800:
            return True
        BAOSTOCK_LOCK.unlink()
    return False


def 创建BaoStock锁():
    BAOSTOCK_LOCK.parent.mkdir(parents=True, exist_ok=True)
    BAOSTOCK_LOCK.write_text(f"15分钟增量更新 - {datetime.datetime.now()}", encoding="utf-8")


def 释放BaoStock锁():
    if BAOSTOCK_LOCK.exists():
        BAOSTOCK_LOCK.unlink()


def 获取最近完整交易日() -> str | None:
    今天 = datetime.date.today()
    开始 = (今天 - datetime.timedelta(days=30)).strftime("%Y-%m-%d")
    结束 = 今天.strftime("%Y-%m-%d")

    rs = bs.query_trade_dates(start_date=开始, end_date=结束)
    交易日列表 = []
    while rs.next():
        row = rs.get_row_data()
        if row[1] == "1":
            交易日列表.append(row[0])

    if not 交易日列表:
        return None

    今天字符串 = 今天.strftime("%Y-%m-%d")
    历史交易日 = [d for d in 交易日列表 if d < 今天字符串]
    if not 历史交易日:
        return None
    return 历史交易日[-1]


def _worker_download(task: dict) -> dict:
    import baostock as bs

    代码 = task["stock_code"]
    名称 = task["stock_name"]
    开始日期 = task["start_date"]
    结束日期 = task["end_date"]
    bs代码 = 代码转BaoStock格式(代码)

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
            os.dup2(old_stderr, 2)
            os.close(old_stderr)
            os.close(devnull_fd)
        return {"ok": False, "stock_code": 代码, "stock_name": 名称, "error": "login failed"}

    rows = []
    fields = None
    error_msg = None

    for 尝试 in range(最大重试次数):
        try:
            rs = bs.query_history_k_data_plus(
                code=bs代码,
                fields=BAOSTOCK_15MIN_FIELDS,
                start_date=开始日期,
                end_date=结束日期,
                frequency=频率15分钟,
                adjustflag=复权方式15分钟,
            )
            if rs.error_code != "0":
                error_msg = rs.error_msg
                if 尝试 < 最大重试次数 - 1:
                    if "未登录" in str(error_msg):
                        try:
                            bs.logout()
                        except Exception:
                            pass
                        old_stdout_retry = sys.stdout
                        sys.stdout = io.StringIO()
                        try:
                            bs.login()
                        finally:
                            sys.stdout = old_stdout_retry
                    time.sleep(重试基础间隔 * (2 ** 尝试))
                    continue
                break

            fields = rs.fields
            while rs.next():
                rows.append(rs.get_row_data())
            break
        except Exception as e:
            error_msg = str(e)
            if 尝试 < 最大重试次数 - 1:
                time.sleep(重试基础间隔 * (2 ** 尝试))

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

    if not rows:
        return {"ok": False, "stock_code": 代码, "stock_name": 名称, "error": error_msg or "no data"}

    return {
        "ok": True,
        "stock_code": 代码,
        "stock_name": 名称,
        "fields": fields,
        "rows": rows,
    }


def 转标准格式(rows, fields, stock_code, stock_name) -> pd.DataFrame:
    df = pd.DataFrame(rows, columns=fields)
    out = pd.DataFrame()
    out["date"] = df["date"]
    out["stock_code"] = stock_code
    out["stock_name"] = stock_name
    out["time"] = df["time"].apply(解析时间戳)
    for col in ["open", "high", "low", "close", "volume", "amount"]:
        out[col] = pd.to_numeric(df[col], errors="coerce")
    return out[输出列名15分钟]


def main():
    log("=" * 70)
    log(f"15分钟数据智能增量更新 - {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log("=" * 70)

    if 检查BaoStock锁():
        log("\n[警告] 检测到其他BaoStock脚本正在运行。")
        log(f"请稍后重试，或确认锁文件是否僵尸：{BAOSTOCK_LOCK}")
        return

    创建BaoStock锁()
    try:
        if not 数据15分钟路径.exists():
            log(f"\n[错误] 15分钟数据文件不存在：{数据15分钟路径}")
            log("请先运行 下载15分钟数据.py 完成全量初始化。")
            return

        log("\n[1/5] 读取现有15分钟数据...")
        现有数据 = pd.read_parquet(数据15分钟路径)
        if 现有数据.empty:
            log("[错误] 现有15分钟数据为空，请先运行全量下载。")
            return
        log(f"  行数: {len(现有数据):,}")
        log(f"  股票数: {现有数据['stock_code'].nunique():,}")
        log(f"  日期范围: {现有数据['date'].min()} ~ {现有数据['date'].max()}")

        log("\n[2/5] 计算各股票最新日期...")
        股票最新日期 = 现有数据.groupby(["stock_code", "stock_name"])["date"].max().reset_index()
        股票最新日期.columns = ["stock_code", "stock_name", "last_date"]

        log("\n[3/5] 获取最近完整交易日...")
        lg = bs.login()
        if lg.error_code != "0":
            log(f"[错误] BaoStock登录失败: {lg.error_msg}")
            return
        最近交易日 = 获取最近完整交易日()
        bs.logout()

        if not 最近交易日:
            log("[错误] 未获取到有效交易日。")
            return
        log(f"  最近完整交易日: {最近交易日}")

        需要更新 = 股票最新日期[股票最新日期["last_date"] < 最近交易日].copy()
        log(f"  需要更新股票: {len(需要更新):,}（已最新: {len(股票最新日期) - len(需要更新):,}）")

        if 需要更新.empty:
            log("\n所有股票15分钟数据都已是最新。")
            return

        log(f"\n[4/5] 并发下载增量数据（{并发进程数} 进程）...")
        任务列表 = []
        for _, row in 需要更新.iterrows():
            任务列表.append(
                {
                    "stock_code": row["stock_code"],
                    "stock_name": row["stock_name"],
                    # 从最后一天重拉一遍，便于修复边界缺失，最终靠去重保证一致性
                    "start_date": row["last_date"],
                    "end_date": 最近交易日,
                }
            )

        新增数据列表 = []
        失败列表 = []
        完成数 = 0
        总任务数 = len(任务列表)

        with ProcessPoolExecutor(max_workers=并发进程数) as executor:
            future_to_task = {executor.submit(_worker_download, t): t for t in 任务列表}
            for future in as_completed(future_to_task):
                task = future_to_task[future]
                完成数 += 1
                代码 = task["stock_code"]

                try:
                    result = future.result()
                    if result.get("ok"):
                        df = 转标准格式(
                            result["rows"], result["fields"], result["stock_code"], result["stock_name"]
                        )
                        if not df.empty:
                            新增数据列表.append(df)
                    else:
                        err = result.get("error", "unknown")
                        if err != "no data":
                            失败列表.append((代码, err))
                except Exception as e:
                    失败列表.append((代码, str(e)))

                if 完成数 % 50 == 0 or 完成数 == 总任务数:
                    log(f"  进度: {完成数}/{总任务数}")

        if not 新增数据列表:
            log("\n未下载到新增15分钟数据。")
            if 失败列表:
                log(f"失败股票数: {len(失败列表)}")
            return

        log("\n[5/5] 合并并保存...")
        新增合并 = pd.concat(新增数据列表, ignore_index=True)
        更新后 = pd.concat([现有数据, 新增合并], ignore_index=True)
        更新前行数 = len(更新后)
        更新后 = 更新后.drop_duplicates(subset=["stock_code", "date", "time"], keep="last")
        去重删除 = 更新前行数 - len(更新后)
        更新后 = 更新后.sort_values(["stock_code", "date", "time"]).reset_index(drop=True)
        更新后.to_parquet(数据15分钟路径, engine="pyarrow", compression="snappy", index=False)

        log("\n" + "=" * 70)
        log("15分钟增量更新完成")
        log(f"  新增抓取行数: {len(新增合并):,}")
        log(f"  去重删除行数: {去重删除:,}")
        log(f"  最终总行数: {len(更新后):,}")
        log(f"  失败股票数: {len(失败列表)}")
        if 失败列表:
            log("  失败样本（前10）:")
            for code, err in 失败列表[:10]:
                log(f"    {code}: {err}")
        log(f"  文件: {数据15分钟路径}")
        log("=" * 70)

    finally:
        释放BaoStock锁()


if __name__ == "__main__":
    mp.freeze_support()
    main()
