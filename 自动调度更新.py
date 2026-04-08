"""自动调度更新（日线 + 上证指数多周期 + 除权缓存 + 月线 + 15分钟 + 30/60分钟派生 + 申万行业数据）。

策略：
- 18:00后按最新完整交易日执行。
- 若错过18:00，后续触发会自动补跑未完成交易日/月或文件修复任务。
- 触发条件 = 目标交易/月份未完成 或 数据文件快照变化。
- 顺序执行：日线 -> 上证指数多周期 -> 除权缓存 -> 月线 -> 15分钟 -> 30/60分钟派生。
- 月线文件始终补到最新日线日期：已完成月份走BaoStock，当前月用日线聚合临时月K。
- 行业/板块数据仅保留申万链路，放在核心价格链路之后执行。
"""

from __future__ import annotations

import datetime
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

import baostock as bs
import pyarrow.parquet as pq

from 配置 import 合并数据路径, 执行时间
from 配置15分钟 import 数据15分钟路径
from 配置月线 import 数据月线路径


项目目录 = Path(__file__).resolve().parent
状态文件 = 项目目录 / "进度" / "自动调度状态.json"
日志文件 = 项目目录 / "进度" / "自动调度日志.txt"
除权数据目录 = 项目目录 / "数据" / "除权数据"
除权事件路径 = 除权数据目录 / "corporate_actions.parquet"
股本变更路径 = 除权数据目录 / "share_changes.parquet"
除权汇总路径 = 除权数据目录 / "fetch_summary.json"
上证指数日线路径 = 项目目录 / "数据" / "上证指数日线数据.parquet"
上证指数15分钟路径 = 项目目录 / "数据" / "上证指数15分钟数据.parquet"
上证指数30分钟路径 = 项目目录 / "数据" / "上证指数30分钟数据.parquet"
上证指数60分钟路径 = 项目目录 / "数据" / "上证指数60分钟数据.parquet"
上证指数月线路径 = 项目目录 / "数据" / "上证指数月线数据.parquet"
申万行业日线路径 = 项目目录 / "数据" / "申万行业日线数据.parquet"
申万二级行业日线路径 = 项目目录 / "数据" / "申万二级行业日线数据.parquet"
申万行业成分股路径 = 项目目录 / "数据" / "申万行业成分股.parquet"

默认触发小时 = 18
默认触发分钟 = 0

调度锁 = Path(tempfile.gettempdir()) / "stock_auto_scheduler.lock"


def log(msg: str) -> None:
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{now}] {msg}"
    print(line, flush=True)
    日志文件.parent.mkdir(parents=True, exist_ok=True)
    with open(日志文件, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def 通知(title: str, message: str) -> None:
    safe_title = title.replace('"', '\\"')
    safe_message = message.replace('"', '\\"')
    script = f'display notification "{safe_message}" with title "{safe_title}"'
    try:
        subprocess.run(["osascript", "-e", script], capture_output=True, text=True)
    except Exception:
        pass


def 读取状态() -> dict:
    if not 状态文件.exists():
        return {}
    try:
        return json.loads(状态文件.read_text(encoding="utf-8"))
    except Exception:
        return {}


def 保存状态(data: dict) -> None:
    状态文件.parent.mkdir(parents=True, exist_ok=True)
    状态文件.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def 文件快照(path: Path) -> dict:
    if not path.exists():
        return {"exists": False, "size": 0, "mtime": 0}
    st = path.stat()
    return {"exists": True, "size": int(st.st_size), "mtime": int(st.st_mtime)}


def 读取JSON(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _格式化日期值(value: object) -> str | None:
    if value is None:
        return None
    if hasattr(value, "as_py"):
        value = value.as_py()
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="ignore")
    if hasattr(value, "to_pydatetime"):
        value = value.to_pydatetime()
    if isinstance(value, datetime.datetime):
        return value.date().isoformat()
    if isinstance(value, datetime.date):
        return value.isoformat()

    text = str(value).strip()
    if not text or text.lower() == "none":
        return None
    if len(text) >= 10 and text[4] == "-" and text[7] == "-":
        return text[:10]
    return None


def _格式化日期时间值(value: object) -> str | None:
    if value is None:
        return None
    if hasattr(value, "as_py"):
        value = value.as_py()
    if hasattr(value, "to_pydatetime"):
        value = value.to_pydatetime()
    if isinstance(value, datetime.datetime):
        return value.date().isoformat()
    if isinstance(value, datetime.date):
        return value.isoformat()

    text = str(value).strip()
    if not text or text.lower() == "none":
        return None
    try:
        return datetime.datetime.fromisoformat(text.replace("Z", "+00:00")).date().isoformat()
    except Exception:
        return None


def 读取Parquet最大日期(path: Path) -> str | None:
    if not path.exists():
        return None

    try:
        pf = pq.ParquetFile(path)
    except Exception:
        return None

    names = pf.metadata.schema.names
    if "date" not in names:
        return None

    date_idx = names.index("date")
    max_date = None
    saw_stats = False

    for i in range(pf.metadata.num_row_groups):
        stats = pf.metadata.row_group(i).column(date_idx).statistics
        if stats is None or stats.max is None:
            continue
        saw_stats = True
        candidate = _格式化日期值(stats.max)
        if candidate and (max_date is None or candidate > max_date):
            max_date = candidate

    if max_date is not None or saw_stats:
        return max_date

    for rb in pf.iter_batches(columns=["date"], batch_size=500_000):
        col = rb.column(0)
        for item in col.to_pylist():
            candidate = _格式化日期值(item)
            if candidate and (max_date is None or candidate > max_date):
                max_date = candidate
    return max_date


def 读取除权缓存日期(summary_path: Path) -> str | None:
    summary = 读取JSON(summary_path)
    return _格式化日期时间值(summary.get("generated_at"))


def 除权缓存快照() -> dict:
    return {
        "actions": 文件快照(除权事件路径),
        "shares": 文件快照(股本变更路径),
        "summary": 文件快照(除权汇总路径),
    }


def 行业数据快照() -> dict:
    return {
        "sw_daily": 文件快照(申万行业日线路径),
        "sw2_daily": 文件快照(申万二级行业日线路径),
        "sw_constituents": 文件快照(申万行业成分股路径),
    }


def 已达到目标日期(actual_date: str | None, target_date: str) -> bool:
    return actual_date is not None and actual_date >= target_date


def 快照变化(旧: dict | None, 新: dict) -> bool:
    if not 旧:
        return True
    return 旧 != 新


def 分钟线依赖日线已变化(m15_state: dict, daily_snap_now: dict) -> bool:
    """分钟线是否需要因为日线快照变化而重新校验/补跑。"""
    return 快照变化(m15_state.get("source_daily_snapshot"), daily_snap_now)


def _同交易日失败且快照未变(state: dict, trade_day: str, snap_now: dict) -> bool:
    return (
        state.get("last_failed_trade_day") == trade_day
        and state.get("last_failed_snapshot") == snap_now
    )


def 获取执行截止时间() -> datetime.time:
    try:
        hour_str, minute_str = 执行时间.split(":")
        return datetime.time(int(hour_str), int(minute_str))
    except Exception:
        return datetime.time(默认触发小时, 默认触发分钟)


def 现在是否已到截止时间() -> bool:
    now = datetime.datetime.now().time()
    return now >= 获取执行截止时间()


def 获取最近完整交易日(*, 截止前排除当日: bool = True) -> str | None:
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
    cutoff = 获取执行截止时间()
    if 截止前排除当日 and datetime.datetime.now().time() < cutoff and trades[-1] == today.strftime("%Y-%m-%d"):
        trades = trades[:-1]
    if not trades:
        return None
    return trades[-1]


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


def 运行脚本(script_name: str) -> bool:
    script_path = 项目目录 / script_name
    if not script_path.exists():
        log(f"[ERROR] 脚本不存在: {script_path}")
        return False

    log(f"[RUN] 开始执行: {script_name}")
    with open(日志文件, "a", encoding="utf-8") as f:
        f.write(f"\n===== START {script_name} =====\n")
        result = subprocess.run(
            [sys.executable, str(script_path)],
            cwd=项目目录,
            stdout=f,
            stderr=subprocess.STDOUT,
            text=True,
        )
        f.write(f"===== END {script_name} (exit={result.returncode}) =====\n\n")

    if result.returncode == 0:
        log(f"[OK] 执行成功: {script_name}")
        return True

    log(f"[ERROR] 执行失败: {script_name}, exit={result.returncode}")
    return False


def 获取调度锁() -> bool:
    try:
        fd = os.open(str(调度锁), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.write(fd, str(os.getpid()).encode("utf-8"))
        os.close(fd)
        return True
    except FileExistsError:
        return False


def 释放调度锁() -> None:
    if 调度锁.exists():
        try:
            调度锁.unlink()
        except Exception:
            pass


def _管道状态(state: dict, name: str) -> dict:
    pipelines = state.setdefault("pipelines", {})
    return pipelines.setdefault(name, {})


def _记录失败状态(pipe_state: dict, trade_day: str, snapshot: dict) -> None:
    pipe_state.update(
        {
            "last_failed_trade_day": trade_day,
            "last_failure_time": datetime.datetime.now().isoformat(),
            "last_failed_snapshot": snapshot,
        }
    )


def _清理失败状态(pipe_state: dict) -> None:
    pipe_state.pop("last_failed_trade_day", None)
    pipe_state.pop("last_failure_time", None)
    pipe_state.pop("last_failed_snapshot", None)


def _应继续修复当日(current_trade_day: str | None, fallback_trade_day: str | None, state: dict, observed_dates: list[str | None]) -> bool:
    if not current_trade_day or not fallback_trade_day or current_trade_day == fallback_trade_day:
        return False

    if any(date == current_trade_day for date in observed_dates if date):
        return True

    pipelines = state.get("pipelines", {})
    for name in ("daily", "index", "exrights", "monthly", "m15", "industry"):
        pipe_state = pipelines.get(name, {})
        if pipe_state.get("last_trade_day") == current_trade_day:
            return True
        if pipe_state.get("last_failed_trade_day") == current_trade_day:
            return True

    return state.get("last_completed_trade_day") == current_trade_day


def main() -> int:
    if not 获取调度锁():
        log("[SKIP] 调度已在运行，跳过本次触发")
        return 0

    try:
        lg = bs.login()
        if lg.error_code != "0":
            log(f"[SKIP] BaoStock登录失败: {lg.error_msg}")
            通知("股票自动更新", "BaoStock登录失败，稍后自动重试")
            return 1

        try:
            trade_day = 获取最近完整交易日(截止前排除当日=True)
            trade_day_latest = 获取最近完整交易日(截止前排除当日=False)
            month_day = 获取最近完整月份交易日()
        finally:
            bs.logout()

        if not trade_day:
            log("[SKIP] 未获取到最近完整交易日，下次自动重试")
            通知("股票自动更新", "未获取到交易日，稍后自动重试")
            return 1

        if not month_day:
            log("[SKIP] 未获取到最近完整月份日期，下次自动重试")
            通知("股票自动更新", "未获取到月线日期，稍后自动重试")
            return 1

        state = 读取状态()
        daily_snap_now = 文件快照(合并数据路径)
        index_snap_now = {
            "daily": 文件快照(上证指数日线路径),
            "m15": 文件快照(上证指数15分钟路径),
            "m30": 文件快照(上证指数30分钟路径),
            "m60": 文件快照(上证指数60分钟路径),
            "monthly": 文件快照(上证指数月线路径),
        }
        monthly_snap_now = 文件快照(数据月线路径)
        m15_snap_now = 文件快照(数据15分钟路径)
        exrights_snap_now = 除权缓存快照()
        daily_max_now = 读取Parquet最大日期(合并数据路径)
        index_max_now = {
            "daily": 读取Parquet最大日期(上证指数日线路径),
            "m15": 读取Parquet最大日期(上证指数15分钟路径),
            "m30": 读取Parquet最大日期(上证指数30分钟路径),
            "m60": 读取Parquet最大日期(上证指数60分钟路径),
            "monthly": 读取Parquet最大日期(上证指数月线路径),
        }
        monthly_max_now = 读取Parquet最大日期(数据月线路径)
        m15_max_now = 读取Parquet最大日期(数据15分钟路径)
        exrights_generated_day = 读取除权缓存日期(除权汇总路径)
        derived_30_max_now = 读取Parquet最大日期(项目目录 / "数据" / "全市场30分钟数据.parquet")
        derived_60_max_now = 读取Parquet最大日期(项目目录 / "数据" / "全市场60分钟数据.parquet")

        if _应继续修复当日(
            trade_day_latest,
            trade_day,
            state,
            [
                daily_max_now,
                monthly_max_now,
                m15_max_now,
                exrights_generated_day,
                derived_30_max_now,
                derived_60_max_now,
                *index_max_now.values(),
            ],
        ):
            log(f"[INFO] 检测到当日未完成更新，提前进入当日修复模式: {trade_day_latest}")
            trade_day = trade_day_latest

        monthly_target_day = trade_day
        daily_fresh = 已达到目标日期(daily_max_now, trade_day)
        index_fresh = all(已达到目标日期(v, trade_day) for v in index_max_now.values())
        exrights_fresh = 已达到目标日期(exrights_generated_day, trade_day)
        monthly_fresh = 已达到目标日期(monthly_max_now, monthly_target_day)
        m15_fresh = 已达到目标日期(m15_max_now, trade_day)
        derived_fresh = 已达到目标日期(derived_30_max_now, trade_day) and 已达到目标日期(derived_60_max_now, trade_day)

        daily_state = _管道状态(state, "daily")
        index_state = _管道状态(state, "index")
        exrights_state = _管道状态(state, "exrights")
        monthly_state = _管道状态(state, "monthly")
        m15_state = _管道状态(state, "m15")
        derived_state = _管道状态(state, "derived")

        need_daily = (daily_state.get("last_trade_day") != trade_day) or 快照变化(daily_state.get("snapshot"), daily_snap_now) or (not daily_fresh)
        need_index = (index_state.get("last_trade_day") != trade_day) or 快照变化(index_state.get("snapshot"), index_snap_now) or (not index_fresh)
        need_exrights = (exrights_state.get("last_trade_day") != trade_day) or 快照变化(exrights_state.get("snapshot"), exrights_snap_now) or (not exrights_fresh)
        need_monthly = (monthly_state.get("last_trade_day") != monthly_target_day) or 快照变化(monthly_state.get("snapshot"), monthly_snap_now) or (not monthly_fresh)
        need_m15 = (
            (m15_state.get("last_trade_day") != trade_day)
            or 快照变化(m15_state.get("snapshot"), m15_snap_now)
            or (not m15_fresh)
            or 分钟线依赖日线已变化(m15_state, daily_snap_now)
        )
        if need_m15 and _同交易日失败且快照未变(m15_state, trade_day, m15_snap_now):
            log(f"[INFO] 15分钟在当前交易日已失败且文件未变化（{trade_day}），本轮继续重试")

        m15_ready_for_derived = m15_state.get("last_trade_day") == trade_day and m15_fresh
        derived_snapshot_now = {
            "source_m15": m15_snap_now,
            "target_30": 文件快照(项目目录 / "数据" / "全市场30分钟数据.parquet"),
            "target_60": 文件快照(项目目录 / "数据" / "全市场60分钟数据.parquet"),
        }
        need_derived = m15_ready_for_derived and (
            derived_state.get("last_trade_day") != trade_day
            or 快照变化(derived_state.get("snapshot"), derived_snapshot_now)
            or (not derived_fresh)
        )

        log(
            "[INFO] 触发判断: "
            f"daily={need_daily} index={need_index} exrights={need_exrights} monthly={need_monthly} m15={need_m15} derived={need_derived} "
            f"(trade_day={trade_day}, monthly_target_day={monthly_target_day}, official_month_day={month_day})"
        )
        log(
            "[INFO] 文件日期: "
            f"daily={daily_max_now or '-'} "
            f"index_d={index_max_now['daily'] or '-'} index_15={index_max_now['m15'] or '-'} "
            f"index_30={index_max_now['m30'] or '-'} index_60={index_max_now['m60'] or '-'} "
            f"index_m={index_max_now['monthly'] or '-'} exrights={exrights_generated_day or '-'} monthly={monthly_max_now or '-'} "
            f"m15={m15_max_now or '-'} m30={derived_30_max_now or '-'} m60={derived_60_max_now or '-'}"
        )

        if not 现在是否已到截止时间():
            if not (need_daily or need_index or need_exrights or need_monthly or need_m15 or need_derived):
                log(f"[SKIP] 当前时间早于{执行时间}，且无待补跑/修复任务")
                return 0
            log(f"[INFO] 当前时间早于{执行时间}，检测到待补跑/修复任务，继续执行")

        did_run_any = False

        if need_daily:
            if not 运行脚本("智能更新.py"):
                _记录失败状态(daily_state, trade_day, 文件快照(合并数据路径))
                保存状态(state)
                通知("股票自动更新失败", f"日线更新失败（{trade_day}）")
                return 1
            did_run_any = True
            daily_success_time = datetime.datetime.now().isoformat()
            daily_snap_after = 文件快照(合并数据路径)
            daily_max_after = 读取Parquet最大日期(合并数据路径)
            if not 已达到目标日期(daily_max_after, trade_day):
                _记录失败状态(daily_state, trade_day, daily_snap_after)
                保存状态(state)
                log(f"[ERROR] 日线脚本返回成功，但文件最大日期仍为 {daily_max_after or '-'}，目标 {trade_day}")
                通知("股票自动更新失败", f"日线文件未更新到目标交易日（{trade_day}）")
                return 1
            daily_snap_now = daily_snap_after
            daily_max_now = daily_max_after
            daily_state.update(
                {
                    "last_trade_day": trade_day,
                    "last_success_time": daily_success_time,
                    "snapshot": daily_snap_after,
                }
            )
            _清理失败状态(daily_state)
            保存状态(state)
        else:
            log(f"[SKIP] 日线已完成且文件未变更（{trade_day}）")

        if need_index:
            if not 运行脚本("智能更新上证指数.py"):
                _记录失败状态(index_state, trade_day, index_snap_now)
                保存状态(state)
                通知("股票自动更新失败", f"上证指数多周期更新失败（{trade_day}）")
                return 1
            did_run_any = True
            index_success_time = datetime.datetime.now().isoformat()
            index_snap_after = {
                "daily": 文件快照(上证指数日线路径),
                "m15": 文件快照(上证指数15分钟路径),
                "m30": 文件快照(上证指数30分钟路径),
                "m60": 文件快照(上证指数60分钟路径),
                "monthly": 文件快照(上证指数月线路径),
            }
            index_max_after = {
                "daily": 读取Parquet最大日期(上证指数日线路径),
                "m15": 读取Parquet最大日期(上证指数15分钟路径),
                "m30": 读取Parquet最大日期(上证指数30分钟路径),
                "m60": 读取Parquet最大日期(上证指数60分钟路径),
                "monthly": 读取Parquet最大日期(上证指数月线路径),
            }
            if not all(已达到目标日期(v, trade_day) for v in index_max_after.values()):
                _记录失败状态(index_state, trade_day, index_snap_after)
                保存状态(state)
                log(
                    "[ERROR] 上证指数脚本返回成功，但部分文件日期仍落后: "
                    f"daily={index_max_after['daily'] or '-'} "
                    f"m15={index_max_after['m15'] or '-'} m30={index_max_after['m30'] or '-'} "
                    f"m60={index_max_after['m60'] or '-'} monthly={index_max_after['monthly'] or '-'} "
                    f"目标={trade_day}"
                )
                通知("股票自动更新失败", f"上证指数多周期文件未更新到目标交易日（{trade_day}）")
                return 1
            index_state.update(
                {
                    "last_trade_day": trade_day,
                    "last_success_time": index_success_time,
                    "snapshot": index_snap_after,
                }
            )
            _清理失败状态(index_state)
            保存状态(state)
        else:
            log(f"[SKIP] 上证指数多周期已完成且文件未变更（{trade_day}）")

        if need_exrights:
            if not 运行脚本("更新除权数据.py"):
                _记录失败状态(exrights_state, trade_day, exrights_snap_now)
                保存状态(state)
                通知("股票自动更新失败", f"除权数据更新失败（{trade_day}）")
                return 1
            did_run_any = True
            exrights_success_time = datetime.datetime.now().isoformat()
            exrights_snap_after = 除权缓存快照()
            exrights_generated_after = 读取除权缓存日期(除权汇总路径)
            if not 已达到目标日期(exrights_generated_after, trade_day):
                _记录失败状态(exrights_state, trade_day, exrights_snap_after)
                保存状态(state)
                log(
                    f"[ERROR] 除权数据脚本返回成功，但汇总日期仍为 {exrights_generated_after or '-'}，目标 {trade_day}"
                )
                通知("股票自动更新失败", f"除权数据未更新到目标交易日（{trade_day}）")
                return 1
            exrights_state.update(
                {
                    "last_trade_day": trade_day,
                    "last_success_time": exrights_success_time,
                    "snapshot": exrights_snap_after,
                }
            )
            _清理失败状态(exrights_state)
            保存状态(state)
        else:
            log(f"[SKIP] 除权数据已完成且文件未变更（{trade_day}）")

        if need_monthly:
            if not 运行脚本("智能更新月线.py"):
                _记录失败状态(monthly_state, monthly_target_day, 文件快照(数据月线路径))
                保存状态(state)
                通知("股票自动更新失败", f"月线更新失败（{monthly_target_day}）")
                return 1
            did_run_any = True
            monthly_success_time = datetime.datetime.now().isoformat()
            monthly_snap_after = 文件快照(数据月线路径)
            monthly_max_after = 读取Parquet最大日期(数据月线路径)
            if not 已达到目标日期(monthly_max_after, monthly_target_day):
                _记录失败状态(monthly_state, monthly_target_day, monthly_snap_after)
                保存状态(state)
                log(f"[ERROR] 月线脚本返回成功，但文件最大日期仍为 {monthly_max_after or '-'}，目标 {monthly_target_day}")
                通知("股票自动更新失败", f"月线文件未更新到最新日线日期（{monthly_target_day}）")
                return 1
            monthly_state.update(
                {
                    "last_trade_day": monthly_target_day,
                    "last_month_day": month_day,
                    "last_success_time": monthly_success_time,
                    "snapshot": monthly_snap_after,
                }
            )
            _清理失败状态(monthly_state)
            保存状态(state)
        else:
            log(f"[SKIP] 月线已完成且文件未变更（{monthly_target_day}）")

        if need_m15:
            if not 运行脚本("智能更新15分钟.py"):
                _记录失败状态(m15_state, trade_day, 文件快照(数据15分钟路径))
                保存状态(state)
                通知("股票自动更新失败", f"15分钟更新失败（{trade_day}）")
                return 1
            did_run_any = True
            m15_success_time = datetime.datetime.now().isoformat()
            m15_snap_after = 文件快照(数据15分钟路径)
            m15_max_after = 读取Parquet最大日期(数据15分钟路径)
            if not 已达到目标日期(m15_max_after, trade_day):
                _记录失败状态(m15_state, trade_day, m15_snap_after)
                保存状态(state)
                log(f"[ERROR] 15分钟脚本返回成功，但文件最大日期仍为 {m15_max_after or '-'}，目标 {trade_day}")
                通知("股票自动更新失败", f"15分钟文件未更新到目标交易日（{trade_day}）")
                return 1
            m15_state.update(
                {
                    "last_trade_day": trade_day,
                    "last_success_time": m15_success_time,
                    "snapshot": m15_snap_after,
                    "source_daily_snapshot": daily_snap_now,
                }
            )
            _清理失败状态(m15_state)
            保存状态(state)
            m15_ready_for_derived = True
            need_derived = True
            derived_snapshot_now["source_m15"] = m15_snap_after
        else:
            log(f"[SKIP] 15分钟已完成且文件未变更（{trade_day}）")

        if need_derived:
            if not 运行脚本("派生分钟数据.py"):
                _记录失败状态(derived_state, trade_day, derived_snapshot_now)
                保存状态(state)
                通知("股票自动更新失败", f"30/60分钟派生失败（{trade_day}）")
                return 1
            did_run_any = True
            derived_success_time = datetime.datetime.now().isoformat()
            derived_30_max_after = 读取Parquet最大日期(项目目录 / "数据" / "全市场30分钟数据.parquet")
            derived_60_max_after = 读取Parquet最大日期(项目目录 / "数据" / "全市场60分钟数据.parquet")
            if not (已达到目标日期(derived_30_max_after, trade_day) and 已达到目标日期(derived_60_max_after, trade_day)):
                _记录失败状态(
                    derived_state,
                    trade_day,
                    {
                        "source_m15": 文件快照(数据15分钟路径),
                        "target_30": 文件快照(项目目录 / "数据" / "全市场30分钟数据.parquet"),
                        "target_60": 文件快照(项目目录 / "数据" / "全市场60分钟数据.parquet"),
                    },
                )
                保存状态(state)
                log(
                    "[ERROR] 30/60分钟派生脚本返回成功，但目标文件日期仍落后: "
                    f"30分钟={derived_30_max_after or '-'} 60分钟={derived_60_max_after or '-'} 目标={trade_day}"
                )
                通知("股票自动更新失败", f"30/60分钟文件未更新到目标交易日（{trade_day}）")
                return 1
            derived_state.update(
                {
                    "last_trade_day": trade_day,
                    "last_success_time": derived_success_time,
                    "snapshot": {
                        "source_m15": 文件快照(数据15分钟路径),
                        "target_30": 文件快照(项目目录 / "数据" / "全市场30分钟数据.parquet"),
                        "target_60": 文件快照(项目目录 / "数据" / "全市场60分钟数据.parquet"),
                    },
                }
            )
            _清理失败状态(derived_state)
            保存状态(state)
        else:
            if not m15_ready_for_derived:
                log(f"[SKIP] 30/60分钟跳过派生（15分钟尚未完成至 {trade_day}）")
            else:
                log(f"[SKIP] 30/60分钟已完成且文件未变更（{trade_day}）")

        if did_run_any:
            final_success_time = datetime.datetime.now().isoformat()
            state.update(
                {
                    "last_completed_trade_day": trade_day,
                    "last_completed_month_day": month_day,
                    "last_success_time": final_success_time,
                    "last_success_host": os.uname().nodename if hasattr(os, "uname") else "",
                }
            )
            保存状态(state)
            log("[DONE] 自动更新流程完成")
            通知("股票自动更新完成", f"已完成日线+上证指数多周期+除权+月线+15/30/60分钟+申万数据更新（{trade_day} / {month_day}）")
        else:
            log("[SKIP] 本轮无实际更新，已跳过完成通知")
        return 0

    finally:
        释放调度锁()


if __name__ == "__main__":
    raise SystemExit(main())
