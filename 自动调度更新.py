"""自动调度更新（日线 + 15分钟增量）

设计目标：
1. 每天18:00后执行一次完整更新（先日线，后15分钟）。
2. 电脑18:00未开机：开机后自动补跑。
3. 网络临时不可用：下次调度自动重试。
4. 避免重复更新：同一最近完整交易日只成功一次。
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


项目目录 = Path(__file__).resolve().parent
状态文件 = 项目目录 / "进度" / "自动调度状态.json"
日志文件 = 项目目录 / "进度" / "自动调度日志.txt"

# 调度阈值：18:00 后允许执行
触发小时 = 18
触发分钟 = 0

# 防并发锁
调度锁 = Path(tempfile.gettempdir()) / "stock_auto_scheduler.lock"


def log(msg: str):
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{now}] {msg}"
    print(line, flush=True)
    日志文件.parent.mkdir(parents=True, exist_ok=True)
    with open(日志文件, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def 通知(title: str, message: str):
    """macOS 通知中心弹窗。"""
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


def 保存状态(data: dict):
    状态文件.parent.mkdir(parents=True, exist_ok=True)
    状态文件.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def 获取最近完整交易日() -> str | None:
    今天 = datetime.date.today()
    开始 = (今天 - datetime.timedelta(days=40)).strftime("%Y-%m-%d")
    结束 = 今天.strftime("%Y-%m-%d")

    lg = bs.login()
    if lg.error_code != "0":
        log(f"[ERROR] BaoStock登录失败: {lg.error_msg}")
        return None

    交易日 = []
    rs = bs.query_trade_dates(start_date=开始, end_date=结束)
    while rs.next():
        d, is_trade = rs.get_row_data()
        if is_trade == "1":
            交易日.append(d)
    bs.logout()

    if not 交易日:
        return None

    今天字符串 = 今天.strftime("%Y-%m-%d")
    历史交易日 = [d for d in 交易日 if d < 今天字符串]
    if not 历史交易日:
        return None
    return 历史交易日[-1]


def 现在是否允许执行() -> bool:
    now = datetime.datetime.now().time()
    return now >= datetime.time(触发小时, 触发分钟)


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


def 释放调度锁():
    if 调度锁.exists():
        try:
            调度锁.unlink()
        except Exception:
            pass


def main():
    if not 获取调度锁():
        log("[SKIP] 调度已在运行，跳过本次触发")
        return

    try:
        if not 现在是否允许执行():
            log("[SKIP] 当前时间早于18:00，等待下一次调度")
            return

        最近完整交易日 = 获取最近完整交易日()
        if not 最近完整交易日:
            log("[SKIP] 未获取到最近完整交易日（网络/API可能不可用），下次自动重试")
            通知("股票自动更新", "未获取到交易日，稍后会自动重试")
            return

        状态 = 读取状态()
        已完成交易日 = 状态.get("last_completed_trade_day", "")
        if 已完成交易日 == 最近完整交易日:
            log(f"[SKIP] 最近完整交易日 {最近完整交易日} 已完成更新")
            return

        log(f"[INFO] 目标交易日: {最近完整交易日}，开始自动更新流程")

        if not 运行脚本("智能更新.py"):
            log("[ERROR] 日线更新失败，本次结束，等待下次自动重试")
            通知("股票自动更新失败", f"日线更新失败（交易日 {最近完整交易日}）")
            return

        if not 运行脚本("智能更新15分钟.py"):
            log("[ERROR] 15分钟更新失败，本次结束，等待下次自动重试")
            通知("股票自动更新失败", f"15分钟更新失败（交易日 {最近完整交易日}）")
            return

        状态.update(
            {
                "last_completed_trade_day": 最近完整交易日,
                "last_success_time": datetime.datetime.now().isoformat(),
                "last_success_host": os.uname().nodename if hasattr(os, "uname") else "",
            }
        )
        保存状态(状态)
        log(f"[DONE] 自动更新完成，已标记交易日: {最近完整交易日}")
        通知("股票自动更新完成", f"已完成日线+15分钟更新（{最近完整交易日}）")

    finally:
        释放调度锁()


if __name__ == "__main__":
    main()
