"""自动调度更新（日线 + 月线 + 15分钟 + 30/60分钟派生）。

策略：
- 18:00后按最新完整交易日执行。
- 若错过18:00，后续触发会自动补跑未完成交易日/月或文件修复任务。
- 触发条件 = 目标交易/月份未完成 或 数据文件快照变化。
- 顺序执行：日线 -> 月线 -> 15分钟 -> 30/60分钟派生。
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

from 配置 import 合并数据路径, 执行时间
from 配置15分钟 import 数据15分钟路径
from 配置月线 import 数据月线路径


项目目录 = Path(__file__).resolve().parent
状态文件 = 项目目录 / "进度" / "自动调度状态.json"
日志文件 = 项目目录 / "进度" / "自动调度日志.txt"

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


def 快照变化(旧: dict | None, 新: dict) -> bool:
    if not 旧:
        return True
    return 旧 != 新


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
    cutoff = 获取执行截止时间()
    if datetime.datetime.now().time() < cutoff and trades[-1] == today.strftime("%Y-%m-%d"):
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
            trade_day = 获取最近完整交易日()
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
        now_iso = datetime.datetime.now().isoformat()

        daily_snap_now = 文件快照(合并数据路径)
        monthly_snap_now = 文件快照(数据月线路径)
        m15_snap_now = 文件快照(数据15分钟路径)

        daily_state = _管道状态(state, "daily")
        monthly_state = _管道状态(state, "monthly")
        m15_state = _管道状态(state, "m15")

        need_daily = (daily_state.get("last_trade_day") != trade_day) or 快照变化(daily_state.get("snapshot"), daily_snap_now)
        need_monthly = (monthly_state.get("last_month_day") != month_day) or 快照变化(monthly_state.get("snapshot"), monthly_snap_now)
        need_m15 = (m15_state.get("last_trade_day") != trade_day) or 快照变化(m15_state.get("snapshot"), m15_snap_now)

        if need_m15 and _同交易日失败且快照未变(m15_state, trade_day, m15_snap_now):
            need_m15 = False
            log(f"[SKIP] 15分钟在当前交易日已失败且文件未变化（{trade_day}），等待文件变化或下个交易日再重试")

        log(
            "[INFO] 触发判断: "
            f"daily={need_daily} monthly={need_monthly} m15={need_m15} "
            f"(trade_day={trade_day}, month_day={month_day})"
        )

        if not 现在是否已到截止时间():
            if not (need_daily or need_monthly or need_m15):
                log(f"[SKIP] 当前时间早于{执行时间}，且无待补跑/修复任务")
                return 0
            log(f"[INFO] 当前时间早于{执行时间}，检测到待补跑/修复任务，继续执行")

        if need_daily:
            if not 运行脚本("智能更新.py"):
                通知("股票自动更新失败", f"日线更新失败（{trade_day}）")
                return 1
            daily_state.update(
                {
                    "last_trade_day": trade_day,
                    "last_success_time": now_iso,
                    "snapshot": 文件快照(合并数据路径),
                }
            )
            保存状态(state)
        else:
            log(f"[SKIP] 日线已完成且文件未变更（{trade_day}）")

        if need_monthly:
            if not 运行脚本("智能更新月线.py"):
                通知("股票自动更新失败", f"月线更新失败（{month_day}）")
                return 1
            monthly_state.update(
                {
                    "last_month_day": month_day,
                    "last_success_time": now_iso,
                    "snapshot": 文件快照(数据月线路径),
                }
            )
            保存状态(state)
        else:
            log(f"[SKIP] 月线已完成且文件未变更（{month_day}）")

        if need_m15:
            if not 运行脚本("智能更新15分钟.py"):
                m15_state.update(
                    {
                        "last_failed_trade_day": trade_day,
                        "last_failure_time": now_iso,
                        "last_failed_snapshot": 文件快照(数据15分钟路径),
                    }
                )
                保存状态(state)
                通知("股票自动更新失败", f"15分钟更新失败（{trade_day}）")
                return 1
            m15_state.update(
                {
                    "last_trade_day": trade_day,
                    "last_success_time": now_iso,
                    "snapshot": 文件快照(数据15分钟路径),
                }
            )
            m15_state.pop("last_failed_trade_day", None)
            m15_state.pop("last_failure_time", None)
            m15_state.pop("last_failed_snapshot", None)
            保存状态(state)
        else:
            log(f"[SKIP] 15分钟已完成且文件未变更（{trade_day}）")

        if not 运行脚本("派生分钟数据.py"):
            通知("股票自动更新失败", f"30/60分钟派生失败（{trade_day}）")
            return 1

        state.update(
            {
                "last_completed_trade_day": trade_day,
                "last_completed_month_day": month_day,
                "last_success_time": now_iso,
                "last_success_host": os.uname().nodename if hasattr(os, "uname") else "",
            }
        )
        保存状态(state)

        log("[DONE] 自动更新流程完成")
        通知("股票自动更新完成", f"已完成日线+月线+15/30/60分钟更新（{trade_day} / {month_day}）")
        return 0

    finally:
        释放调度锁()


if __name__ == "__main__":
    raise SystemExit(main())
