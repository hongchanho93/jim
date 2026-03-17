"""安装/卸载自动任务（Windows + macOS）"""

import os
import platform
import plistlib
import shlex
import subprocess
import sys
from pathlib import Path

from 配置 import 计划任务名称, 执行时间, 项目目录


BAT路径 = 项目目录 / "启动更新.bat"
调度脚本路径 = 项目目录 / "自动调度更新.py"
LAUNCHD_LABEL = "com.hongjian.stockdata.autoupdate"
任务日志路径 = Path.home() / "Library" / "Logs" / f"{LAUNCHD_LABEL}.log"
LAUNCHD_PLIST = Path.home() / "Library" / "LaunchAgents" / f"{LAUNCHD_LABEL}.plist"


def _run(cmd: list[str], check: bool = False) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, capture_output=True, text=True, check=check)


def _解析执行时间() -> tuple[int, int]:
    try:
        hour_str, minute_str = 执行时间.split(":")
        return int(hour_str), int(minute_str)
    except Exception:
        return 18, 0


def _生成半小时日历触发点() -> list[dict[str, int]]:
    触发点: list[dict[str, int]] = []
    for hour in range(24):
        for minute in (0, 30):
            触发点.append({"Hour": hour, "Minute": minute})
    return 触发点


def 安装_windows():
    print(f"正在安装Windows计划任务: {计划任务名称}")
    print(f"  执行时间: 每天 {执行时间}")
    print(f"  执行脚本: {BAT路径}")

    cmd = [
        "schtasks",
        "/create",
        "/tn",
        计划任务名称,
        "/tr",
        str(BAT路径),
        "/sc",
        "daily",
        "/st",
        执行时间,
        "/rl",
        "highest",
        "/f",
    ]
    result = _run(cmd)
    if result.returncode == 0:
        print("计划任务安装成功")
    else:
        print(f"安装失败: {result.stderr.strip()}")


def 卸载_windows():
    print(f"正在卸载Windows计划任务: {计划任务名称}")
    result = _run(["schtasks", "/delete", "/tn", 计划任务名称, "/f"])
    if result.returncode == 0:
        print("计划任务已卸载")
    else:
        print(f"卸载失败: {result.stderr.strip()}")


def 状态_windows():
    result = _run(["schtasks", "/query", "/tn", 计划任务名称, "/fo", "list"])
    if result.returncode == 0:
        print(result.stdout.strip())
    else:
        print("未查询到该任务或查询失败")
        if result.stderr.strip():
            print(result.stderr.strip())


def 安装_macos():
    if not 调度脚本路径.exists():
        print(f"[错误] 调度脚本不存在: {调度脚本路径}")
        print("请确认 自动调度更新.py 已存在")
        return

    任务日志路径.parent.mkdir(parents=True, exist_ok=True)
    LAUNCHD_PLIST.parent.mkdir(parents=True, exist_ok=True)

    python_exec = Path(sys.executable).resolve()
    启动命令 = "exec {python} {script}".format(
        python=shlex.quote(str(python_exec)),
        script=shlex.quote(str(调度脚本路径)),
    )

    plist_data = {
        "Label": LAUNCHD_LABEL,
        "ProgramArguments": ["/bin/zsh", "-lc", 启动命令],
        "WorkingDirectory": str(Path.home()),
        "EnvironmentVariables": {
            "PATH": os.environ.get("PATH", "/usr/bin:/bin:/usr/sbin:/sbin"),
            "LANG": os.environ.get("LANG", "zh_CN.UTF-8"),
            "LC_ALL": os.environ.get("LC_ALL", "zh_CN.UTF-8"),
            "PYTHONUTF8": "1",
        },
        # 全天整点/半点触发，由脚本内部判断是否需要补跑
        "StartCalendarInterval": _生成半小时日历触发点(),
        "StandardOutPath": str(任务日志路径),
        "StandardErrorPath": str(任务日志路径),
    }

    with open(LAUNCHD_PLIST, "wb") as f:
        plistlib.dump(plist_data, f)

    uid = str(os.getuid())
    _run(["launchctl", "bootout", f"gui/{uid}", str(LAUNCHD_PLIST)])
    result = _run(["launchctl", "bootstrap", f"gui/{uid}", str(LAUNCHD_PLIST)])
    if result.returncode != 0:
        print(f"安装失败: {result.stderr.strip()}")
        return
    _run(["launchctl", "enable", f"gui/{uid}/{LAUNCHD_LABEL}"])

    print("macOS自动任务安装成功")
    print(f"  Label: {LAUNCHD_LABEL}")
    print(f"  Plist: {LAUNCHD_PLIST}")
    print(f"  调度脚本: {调度脚本路径}")
    print(f"  Python: {python_exec}")
    print(f"  日志: {任务日志路径}")
    print(f"  固定时间: 每天 {执行时间}")
    print("  补偿机制: 全天整点/半点自然调度（脚本内部按交易日自动补跑）")
    print("  安装后不会立即触发，等待下一次自然调度")


def 卸载_macos():
    uid = str(os.getuid())
    _run(["launchctl", "bootout", f"gui/{uid}", str(LAUNCHD_PLIST)])
    if LAUNCHD_PLIST.exists():
        LAUNCHD_PLIST.unlink()
    print("macOS自动任务已卸载")


def 状态_macos():
    uid = str(os.getuid())
    print(f"Plist存在: {LAUNCHD_PLIST.exists()} ({LAUNCHD_PLIST})")
    result = _run(["launchctl", "print", f"gui/{uid}/{LAUNCHD_LABEL}"])
    if result.returncode == 0:
        print("任务状态: 已加载")
    else:
        print("任务状态: 未加载")
    if 任务日志路径.exists():
        print(f"日志文件: {任务日志路径}")


def 安装():
    system = platform.system().lower()
    if system == "windows":
        安装_windows()
    elif system == "darwin":
        安装_macos()
    else:
        print(f"当前系统暂不支持自动安装: {platform.system()}")


def 卸载():
    system = platform.system().lower()
    if system == "windows":
        卸载_windows()
    elif system == "darwin":
        卸载_macos()
    else:
        print(f"当前系统暂不支持自动卸载: {platform.system()}")


def 状态():
    system = platform.system().lower()
    if system == "windows":
        状态_windows()
    elif system == "darwin":
        状态_macos()
    else:
        print(f"当前系统暂不支持状态查询: {platform.system()}")


def 打印用法():
    print("用法:")
    print("  python3 自动任务.py --安装    安装自动更新任务")
    print("  python3 自动任务.py --卸载    卸载自动更新任务")
    print("  python3 自动任务.py --状态    查看自动更新任务状态")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        打印用法()
        sys.exit(1)

    参数 = sys.argv[1]
    if 参数 == "--安装":
        安装()
    elif 参数 == "--卸载":
        卸载()
    elif 参数 == "--状态":
        状态()
    else:
        print(f"未知参数: {参数}")
        打印用法()
        sys.exit(1)
