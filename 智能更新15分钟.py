"""15分钟智能增量更新（含清洗 + 低内存合并）。"""

from __future__ import annotations

import argparse
import datetime
import io
import os
import shutil
import subprocess
import signal
import sys
import tempfile
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import baostock as bs
import multiprocessing as mp
import pandas as pd
import pyarrow.parquet as pq

from 配置 import 合并数据路径
from 配置 import 执行时间
from 配置15分钟 import (
    BAOSTOCK_15MIN_FIELDS,
    复权方式15分钟,
    最大重试次数,
    输出列名15分钟,
    开始日期,
    数据15分钟路径,
    频率15分钟,
    重试基础间隔,
)
from 派生分钟数据 import 增量更新派生分钟数据
from 数据清洗规则 import 清洗15分钟


并发进程数 = 3
批任务大小 = 80
逐股验收再跑轮数 = 1
BAOSTOCK_LOCK = Path(tempfile.gettempdir()) / "baostock.lock"
单股查询超时秒数 = 120


def _完整交易日截止时间() -> datetime.time:
    try:
        hour_str, minute_str = 执行时间.split(":")
        return datetime.time(int(hour_str), int(minute_str))
    except Exception:
        return datetime.time(18, 0)


def log(msg: str) -> None:
    try:
        print(msg, flush=True)
    except UnicodeEncodeError:
        print(msg.encode("gbk", errors="replace").decode("gbk"), flush=True)


def 代码转BaoStock格式(stock_code: str) -> str:
    if stock_code.startswith(("6", "9")):
        return f"sh.{stock_code}"
    return f"sz.{stock_code}"


def 解析时间戳(raw: str) -> str:
    x = str(raw or "").strip()
    if len(x) >= 14 and x[:14].isdigit():
        return f"{x[8:10]}:{x[10:12]}:{x[12:14]}"
    if len(x) == 8 and x.count(":") == 2:
        return x
    return ""


def 检查BaoStock锁() -> bool:
    if BAOSTOCK_LOCK.exists():
        mtime = BAOSTOCK_LOCK.stat().st_mtime
        if time.time() - mtime < 1800:
            return True
        BAOSTOCK_LOCK.unlink(missing_ok=True)
    return False


def 创建BaoStock锁() -> None:
    BAOSTOCK_LOCK.parent.mkdir(parents=True, exist_ok=True)
    BAOSTOCK_LOCK.write_text(f"15m-update {datetime.datetime.now().isoformat()}", encoding="utf-8")


def 释放BaoStock锁() -> None:
    BAOSTOCK_LOCK.unlink(missing_ok=True)


def 获取最近完整交易日() -> str | None:
    today = datetime.date.today()
    start = (today - datetime.timedelta(days=40)).strftime("%Y-%m-%d")
    end = today.strftime("%Y-%m-%d")

    rs = bs.query_trade_dates(start_date=start, end_date=end)
    trades: list[str] = []
    while rs.next():
        d, is_trade = rs.get_row_data()
        if is_trade == "1":
            trades.append(d)

    if not trades:
        return None
    trades = sorted(trades)
    if datetime.datetime.now().time() < _完整交易日截止时间() and trades[-1] == today.strftime("%Y-%m-%d"):
        trades = trades[:-1]
    if not trades:
        return None
    return trades[-1]


def _有效股票代码(series: pd.Series) -> pd.Series:
    s = series.fillna("").astype(str).str.strip()
    return s.str.fullmatch(r"\d{6}", na=False)


def _从日线构建股票宇宙() -> pd.DataFrame:
    if not 合并数据路径.exists():
        raise FileNotFoundError(f"日线文件不存在: {合并数据路径}")

    df = pd.read_parquet(
        合并数据路径,
        columns=["stock_code", "stock_name", "date", "volume", "amount"],
    )
    df["stock_code"] = df["stock_code"].astype(str).str.strip()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df[_有效股票代码(df["stock_code"]) & df["date"].notna()].copy()
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0.0)
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0.0)

    latest = (
        df.sort_values(["stock_code", "date"], kind="mergesort")
        .drop_duplicates(subset=["stock_code"], keep="last")
        .loc[:, ["stock_code", "stock_name", "date", "volume", "amount"]]
        .rename(columns={"date": "daily_last"})
        .reset_index(drop=True)
    )
    latest["daily_last"] = latest["daily_last"].dt.strftime("%Y-%m-%d")

    traded = (
        df[(df["volume"] > 0) | (df["amount"] > 0)]
        .groupby("stock_code", as_index=False)["date"]
        .max()
        .rename(columns={"date": "minute_target_last"})
    )
    latest = latest.merge(traded, on="stock_code", how="left")
    latest["minute_target_last"] = pd.to_datetime(latest["minute_target_last"], errors="coerce").dt.strftime("%Y-%m-%d")
    latest["minute_target_last"] = latest["minute_target_last"].fillna(latest["daily_last"])
    return latest.loc[:, ["stock_code", "stock_name", "daily_last", "minute_target_last"]]


def _从15分钟文件构建最后日期(path: Path, batch_size: int = 700_000) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=["stock_code", "stock_name", "m15_last"])

    pf = pq.ParquetFile(path)
    date_map: dict[str, str] = {}
    name_map: dict[str, str] = {}

    for rb in pf.iter_batches(columns=["stock_code", "stock_name", "date"], batch_size=batch_size):
        df = rb.to_pandas()
        if df.empty:
            continue

        df["stock_code"] = df["stock_code"].astype(str).str.strip()
        df = df[_有效股票代码(df["stock_code"])].copy()
        if df.empty:
            continue

        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
        df = df[df["date"].notna()].copy()
        if df.empty:
            continue

        g = df.groupby("stock_code", as_index=False)["date"].max()
        last_name = (
            df.sort_values(["stock_code", "date"], kind="mergesort")
            .drop_duplicates(subset=["stock_code"], keep="last")
            .loc[:, ["stock_code", "stock_name", "date"]]
        )

        name_by_code = dict(zip(last_name["stock_code"], last_name["stock_name"]))
        for _, r in g.iterrows():
            code = str(r["stock_code"])
            d = str(r["date"])
            old = date_map.get(code)
            if old is None or d > old:
                date_map[code] = d
                name_map[code] = str(name_by_code.get(code, name_map.get(code, "")))

    if not date_map:
        return pd.DataFrame(columns=["stock_code", "stock_name", "m15_last"])

    out = pd.DataFrame(
        {
            "stock_code": list(date_map.keys()),
            "stock_name": [name_map.get(c, "") for c in date_map.keys()],
            "m15_last": [date_map[c] for c in date_map.keys()],
        }
    )
    return out


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
        start_date = task["start_date"]
        end_date = task["end_date"]
        bs_code = 代码转BaoStock格式(code)
        print(f"[worker] query {code} {name} {start_date}->{end_date}", flush=True)

        rows = []
        fields = None
        err = None

        for i in range(最大重试次数):
            try:
                signal.signal(signal.SIGALRM, _超时处理)
                signal.alarm(单股查询超时秒数)
                rs = bs.query_history_k_data_plus(
                    code=bs_code,
                    fields=BAOSTOCK_15MIN_FIELDS,
                    start_date=start_date,
                    end_date=end_date,
                    frequency=频率15分钟,
                    adjustflag=复权方式15分钟,
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

        if not rows:
            results.append({"ok": False, "stock_code": code, "stock_name": name, "error": err or "no data"})
            continue

        results.append(
            {
                "ok": True,
                "stock_code": code,
                "stock_name": name,
                "fields": fields,
                "rows": rows,
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


def 转标准格式(rows: list[list[str]], fields: list[str], stock_code: str, stock_name: str) -> pd.DataFrame:
    df = pd.DataFrame(rows, columns=fields)
    out = pd.DataFrame()
    out["date"] = df["date"]
    out["stock_code"] = stock_code
    out["stock_name"] = stock_name
    out["time"] = df["time"].map(解析时间戳)

    for c in ["open", "high", "low", "close", "volume", "amount"]:
        out[c] = pd.to_numeric(df[c], errors="coerce")

    return out[输出列名15分钟]


def _调用低内存合并(batch_dir: Path, output: Path, work_dir: Path) -> None:
    cmd = [
        sys.executable,
        str(Path(__file__).resolve().parent / "merge_15min_lowmem.py"),
        "--batch-dir",
        str(batch_dir),
        "--output",
        str(output),
        "--work-dir",
        str(work_dir),
        "--prefix-width",
        "4",
    ]
    r = subprocess.run(cmd, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"low-memory merge failed: exit={r.returncode}")


def _硬链接或复制(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists():
        dst.unlink()
    try:
        os.link(src, dst)
    except Exception:
        shutil.copy2(src, dst)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="15分钟智能增量更新")
    p.add_argument("--dry-run", action="store_true", help="只下载与校验，不写回文件")
    p.add_argument("--limit-stocks", type=int, default=0, help="调试：限制更新股票数")
    return p.parse_args()


def _计算需更新股票(
    daily_latest: pd.DataFrame,
    m15_latest: pd.DataFrame,
    最近交易日: str,
) -> pd.DataFrame:
    joined = daily_latest.merge(
        m15_latest.loc[:, ["stock_code", "m15_last"]],
        on="stock_code",
        how="left",
    )
    target = joined["minute_target_last"].fillna(joined["daily_last"])
    if 最近交易日:
        target = target.where(target <= 最近交易日, 最近交易日)
    joined["minute_target_last"] = target
    need = joined[joined["m15_last"].isna() | (joined["m15_last"] < joined["minute_target_last"])].copy()
    return need.sort_values(["minute_target_last", "m15_last", "stock_code"], na_position="first").reset_index(drop=True)


def _打印逐股验收(need: pd.DataFrame, latest: pd.DataFrame, 最近交易日: str) -> pd.DataFrame:
    验收 = need.loc[:, ["stock_code", "stock_name", "minute_target_last", "m15_last"]].merge(
        latest.loc[:, ["stock_code", "m15_last"]].rename(columns={"m15_last": "file_last_date"}),
        on="stock_code",
        how="left",
    )
    验收["status"] = "ok"
    验收.loc[验收["file_last_date"].isna() | (验收["file_last_date"] < 验收["minute_target_last"]), "status"] = "lagging"

    ok_count = int((验收["status"] == "ok").sum())
    lagging_count = int((验收["status"] == "lagging").sum())
    log("\n[验收] 逐股更新结果:")
    log(f"  待验收股票数: {len(验收)}")
    log(f"  已到目标日: {ok_count}")
    log(f"  仍落后目标日: {lagging_count}")

    def _fmt_date(value: object) -> str:
        return str(value) if pd.notna(value) else "-"

    if lagging_count:
        log("  落后样本（全部）:")
        for _, row in 验收[验收["status"] == "lagging"].iterrows():
            log(
                f"    {row['stock_code']} {row['stock_name']}: "
                f"last_date={_fmt_date(row['file_last_date'])} target={_fmt_date(row['minute_target_last']) if pd.notna(row['minute_target_last']) else 最近交易日}"
            )
    return 验收


def _补跑滞后股票(
    当前文件索引: pd.DataFrame,
    滞后股票: pd.DataFrame,
    最近交易日: str,
) -> tuple[pd.DataFrame, dict[str, str]]:
    if 滞后股票.empty:
        return (
            pd.DataFrame(columns=["stock_code", "stock_name", "date", "time", "open", "high", "low", "close", "volume", "amount"]),
            {},
        )

    lagging_codes = 滞后股票["stock_code"].astype(str).tolist()
    targets = 当前文件索引[当前文件索引["stock_code"].isin(lagging_codes)].copy()
    if targets.empty:
        return (
            pd.DataFrame(columns=["stock_code", "stock_name", "date", "time", "open", "high", "low", "close", "volume", "amount"]),
            {},
        )

    tasks = []
    for _, r in targets.iterrows():
        start_date = r["m15_last"] if isinstance(r["m15_last"], str) and r["m15_last"] else 开始日期
        tasks.append(
            {
                "stock_code": r["stock_code"],
                "stock_name": r["stock_name"],
                "start_date": start_date,
                "end_date": 最近交易日,
                "last_date": r["m15_last"] if isinstance(r["m15_last"], str) else None,
            }
        )

    log(f"\n[补跑] 针对 {len(tasks)} 只落后股票立即再跑一轮")
    batches = [tasks[i : i + 批任务大小] for i in range(0, len(tasks), 批任务大小)]
    新增列表: list[pd.DataFrame] = []
    失败列表: list[tuple[str, str]] = []
    源端无新增代码: dict[str, str] = {}
    done = 0

    with ProcessPoolExecutor(max_workers=并发进程数) as ex:
        fut_map = {ex.submit(_worker_download_batch, batch): batch for batch in batches}
        for fut in as_completed(fut_map):
            batch_tasks = fut_map[fut]
            try:
                results = fut.result()
            except Exception as e:
                for t in batch_tasks:
                    失败列表.append((t["stock_code"], f"batch error: {e}"))
                    done += 1
                log(f"  [补跑进度] {done}/{len(tasks)}")
                continue

            for r in results:
                done += 1
                code = r["stock_code"]
                if not r.get("ok"):
                    err = r.get("error", "unknown")
                    if err == "no data":
                        t = next((item for item in batch_tasks if item["stock_code"] == code), None)
                        if t is not None:
                            源端无新增代码[code] = t["stock_name"]
                    else:
                        失败列表.append((code, err))
                    if done % 50 == 0 or done == len(tasks):
                        log(f"  [补跑进度] {done}/{len(tasks)}")
                    continue

                t = next((item for item in batch_tasks if item["stock_code"] == code), None)
                df = 转标准格式(r["rows"], r["fields"], r["stock_code"], r["stock_name"])
                last_d = t.get("last_date") if t else None
                if last_d:
                    df = df[df["date"] >= last_d]
                if not df.empty:
                    新增列表.append(df)
                else:
                    if t is not None:
                        源端无新增代码[code] = t["stock_name"]
                if done % 50 == 0 or done == len(tasks):
                    log(f"  [补跑进度] {done}/{len(tasks)}")

    if 失败列表:
        log(f"  [补跑] 仍有失败股票数: {len(失败列表)}")

    if not 新增列表:
        return (
            pd.DataFrame(columns=["stock_code", "stock_name", "date", "time", "open", "high", "low", "close", "volume", "amount"]),
            源端无新增代码,
        )

    return pd.concat(新增列表, ignore_index=True), 源端无新增代码


def _低内存写回15分钟(新增清洗后: pd.DataFrame) -> None:
    temp_root = Path(tempfile.gettempdir()) / f"m15_retry_{int(time.time())}"
    batch_dir = temp_root / "batches"
    work_dir = temp_root / "work"
    output_file = temp_root / "merged.parquet"
    batch_dir.mkdir(parents=True, exist_ok=True)

    try:
        _硬链接或复制(数据15分钟路径, batch_dir / "batch_000000.parquet")
        新增清洗后.to_parquet(batch_dir / "batch_000001.parquet", engine="pyarrow", compression="snappy", index=False)

        _调用低内存合并(batch_dir=batch_dir, output=output_file, work_dir=work_dir)

        target_tmp = 数据15分钟路径.with_suffix(数据15分钟路径.suffix + ".tmp")
        if target_tmp.exists():
            target_tmp.unlink()
        shutil.move(str(output_file), str(target_tmp))
        target_tmp.replace(数据15分钟路径)
    finally:
        shutil.rmtree(temp_root, ignore_errors=True)


def main() -> None:
    args = parse_args()

    log("=" * 70)
    log(f"15分钟智能更新 - {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log("=" * 70)
    log(f"dry_run={args.dry_run}, limit_stocks={args.limit_stocks}")

    if 检查BaoStock锁():
        log(f"[警告] 检测到其他BaoStock脚本正在运行: {BAOSTOCK_LOCK}")
        return

    if not 数据15分钟路径.exists():
        log(f"[错误] 15分钟文件不存在: {数据15分钟路径}")
        return

    创建BaoStock锁()
    try:
        log("\n[1/7] 读取日线股票宇宙...")
        daily_latest = _从日线构建股票宇宙()
        log(f"  日线股票数: {len(daily_latest):,}")

        log("\n[2/7] 构建15分钟最后日期索引（流式）...")
        m15_latest = _从15分钟文件构建最后日期(数据15分钟路径)
        log(f"  15分钟股票数: {len(m15_latest):,}")

        log("\n[3/7] 获取最近完整交易日...")
        lg = bs.login()
        if lg.error_code != "0":
            log(f"[错误] BaoStock登录失败: {lg.error_msg}")
            return
        最近交易日 = 获取最近完整交易日()
        bs.logout()

        if not 最近交易日:
            log("[错误] 未获取到最近完整交易日")
            return
        log(f"  最近完整交易日: {最近交易日}")

        log("\n[4/7] 计算缺失/滞后股票...")
        need = _计算需更新股票(daily_latest, m15_latest, 最近交易日)

        if args.limit_stocks > 0:
            need = need.head(args.limit_stocks).copy()
            log(f"  [调试] 限制更新股票数: {len(need)}")

        log(f"  需更新股票: {len(need):,} / {len(daily_latest):,}")
        if need.empty:
            log("\n所有股票15分钟数据已最新。")
            return

        log(f"\n[5/7] 并发下载增量数据（{并发进程数} 进程）...")
        tasks = []
        for _, r in need.iterrows():
            last = r["m15_last"] if isinstance(r["m15_last"], str) else None
            start_date = last if last else 开始日期
            tasks.append(
                {
                    "stock_code": r["stock_code"],
                    "stock_name": r["stock_name"],
                    "start_date": start_date,
                    "end_date": 最近交易日,
                    "last_date": last,
                }
            )

        新增列表: list[pd.DataFrame] = []
        失败列表: list[tuple[str, str]] = []
        done = 0

        batches = [tasks[i : i + 批任务大小] for i in range(0, len(tasks), 批任务大小)]
        with ProcessPoolExecutor(max_workers=并发进程数) as ex:
            fut_map = {ex.submit(_worker_download_batch, batch): batch for batch in batches}
            for fut in as_completed(fut_map):
                batch_tasks = fut_map[fut]
                try:
                    results = fut.result()
                except Exception as e:
                    for t in batch_tasks:
                        失败列表.append((t["stock_code"], f"batch error: {e}"))
                        done += 1
                    log(f"  进度: {done}/{len(tasks)}")
                    continue

                for r in results:
                    done += 1
                    code = r["stock_code"]
                    if not r.get("ok"):
                        err = r.get("error", "unknown")
                        if err != "no data":
                            失败列表.append((code, err))
                        if done % 50 == 0 or done == len(tasks):
                            log(f"  进度: {done}/{len(tasks)}")
                        continue

                    t = next((item for item in batch_tasks if item["stock_code"] == code), None)
                    df = 转标准格式(r["rows"], r["fields"], r["stock_code"], r["stock_name"])
                    last_d = t.get("last_date") if t else None
                    if last_d:
                        df = df[df["date"] >= last_d]
                    if not df.empty:
                        新增列表.append(df)

                    if done % 50 == 0 or done == len(tasks):
                        log(f"  进度: {done}/{len(tasks)}")

        # 对失败的股票串行重试（独立BaoStock会话，避免并发连接争用）
        if 失败列表:
            log(f"\n[重试] {len(失败列表)} 个失败股票串行重试（独立会话）...")
            still_失败: list[tuple[str, str]] = []
            task_map = {t["stock_code"]: t for t in tasks}
            for code, err_orig in 失败列表:
                t = task_map.get(code)
                if t is None:
                    still_失败.append((code, err_orig))
                    continue
                retry_results = _worker_download_batch([t])
                r = retry_results[0] if retry_results else {"ok": False, "error": "retry returned no result"}
                if r.get("ok"):
                    df = 转标准格式(r["rows"], r["fields"], r["stock_code"], r["stock_name"])
                    last_d = t.get("last_date")
                    if last_d:
                        df = df[df["date"] >= last_d]
                    if not df.empty:
                        新增列表.append(df)
                    log(f"    [重试成功] {code}")
                else:
                    still_失败.append((code, r.get("error", "unknown")))
            失败列表 = still_失败
            if 失败列表:
                log(f"  重试后仍失败: {len(失败列表)}")
            else:
                log("  所有失败股票重试成功")

        if not 新增列表:
            if 失败列表:
                log("\n[错误] 未下载到新增15分钟数据，且仍存在真实失败股票。")
                log(f"失败股票数: {len(失败列表)}")
                log("失败样本（前10）:")
                for code, err in 失败列表[:10]:
                    log(f"    {code}: {err}")
                raise SystemExit(2)

            log("\n[提示] 未下载到新增15分钟数据，但滞后股票均已判定为源端无更晚数据。")
            return

        log("\n[6/7] 清洗新增数据...")
        新增合并 = pd.concat(新增列表, ignore_index=True)
        新增清洗后, 新增报告 = 清洗15分钟(新增合并)
        log(f"  新增原始行数: {len(新增合并):,}")
        log(f"  新增清洗后行数: {len(新增清洗后):,}")
        log(f"  新增清洗后股票数: {新增清洗后['stock_code'].nunique():,}")
        log(f"  清洗修复(all_zero_bar_rows): {新增报告.get('fixes', {}).get('all_zero_bar_rows', 0):,}")

        if args.dry_run:
            log("\n[DRY-RUN] 不写回15分钟文件")
            log(f"  失败股票数: {len(失败列表)}")
            return

        log("\n[7/7] 低内存合并并写回...")
        temp_root = Path(tempfile.gettempdir()) / f"m15_increment_{int(time.time())}"
        batch_dir = temp_root / "batches"
        work_dir = temp_root / "work"
        output_file = temp_root / "merged.parquet"
        batch_dir.mkdir(parents=True, exist_ok=True)

        try:
            _硬链接或复制(数据15分钟路径, batch_dir / "batch_000000.parquet")
            新增清洗后.to_parquet(batch_dir / "batch_000001.parquet", engine="pyarrow", compression="snappy", index=False)

            _调用低内存合并(batch_dir=batch_dir, output=output_file, work_dir=work_dir)

            target_tmp = 数据15分钟路径.with_suffix(数据15分钟路径.suffix + ".tmp")
            if target_tmp.exists():
                target_tmp.unlink()
            shutil.move(str(output_file), str(target_tmp))
            target_tmp.replace(数据15分钟路径)
        finally:
            shutil.rmtree(temp_root, ignore_errors=True)

        log("\n[8/8] 同步生成30分钟和60分钟...")
        增量更新派生分钟数据(新增清洗后, logger=log)

        log("\n" + "=" * 70)
        log("15分钟更新完成")
        log(f"  新增行数(清洗后): {len(新增清洗后):,}")
        log(f"  失败股票数: {len(失败列表)}")
        if 失败列表:
            log("  失败样本（前10）:")
            for code, err in 失败列表[:10]:
                log(f"    {code}: {err}")
        log(f"  文件: {数据15分钟路径}")
        log("=" * 70)

        最新索引 = _从15分钟文件构建最后日期(数据15分钟路径)
        验收结果 = _打印逐股验收(need, 最新索引, 最近交易日)
        未完成 = 验收结果[验收结果["status"] == "lagging"].copy()

        for retry_round in range(逐股验收再跑轮数):
            if 未完成.empty:
                break

            log(f"\n[补跑] 发现 {len(未完成)} 只股票15分钟仍未追平，立即重跑第 {retry_round + 1} 轮")
            补跑清洗后, 补跑源端无新增代码 = _补跑滞后股票(最新索引, 未完成.loc[:, ["stock_code", "stock_name"]], 最近交易日)
            if 补跑清洗后.empty:
                log("[补跑] 未抓到可补数据")
                break

            if 补跑源端无新增代码:
                log(f"  补跑源端无新增股票数: {len(补跑源端无新增代码)}")
            补跑清洗后, 补跑报告 = 清洗15分钟(补跑清洗后)
            log(f"  补跑清洗后行数: {len(补跑清洗后):,}")
            log(f"  补跑清洗修复(all_zero_bar_rows): {补跑报告.get('fixes', {}).get('all_zero_bar_rows', 0):,}")

            _低内存写回15分钟(补跑清洗后)
            log(f"[OK] 补跑后已重写: {数据15分钟路径}")
            log("\n[补跑] 同步生成30分钟和60分钟...")
            增量更新派生分钟数据(补跑清洗后, logger=log)

            最新索引 = _从15分钟文件构建最后日期(数据15分钟路径)
            need = need.copy()
            if 补跑源端无新增代码:
                need = need[~need["stock_code"].isin(补跑源端无新增代码.keys())].copy()
            验收结果 = _打印逐股验收(need, 最新索引, 最近交易日)
            未完成 = 验收结果[验收结果["status"] == "lagging"].copy()

        if not 未完成.empty:
            log(f"[错误] 仍有股票15分钟数据未补齐，视为本次更新失败")
            log(f"  未补齐股票数: {len(未完成)}")
            for _, row in 未完成.iterrows():
                log(f"    {row['stock_code']}: m15_last={row['m15_last']}")
            raise SystemExit(2)

    finally:
        释放BaoStock锁()


if __name__ == "__main__":
    mp.freeze_support()
    main()
