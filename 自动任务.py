"""安装/卸载 Windows 计划任务"""

import sys
import subprocess
from 配置 import 计划任务名称, 执行时间, 项目目录


BAT路径 = 项目目录 / "启动更新.bat"


def 安装():
    """注册Windows计划任务"""
    print(f"正在安装计划任务: {计划任务名称}")
    print(f"  执行时间: 每天 {执行时间}")
    print(f"  执行脚本: {BAT路径}")

    cmd = [
        "schtasks", "/create",
        "/tn", 计划任务名称,
        "/tr", str(BAT路径),
        "/sc", "daily",
        "/st", 执行时间,
        "/rl", "highest",      # 最高权限运行
        "/f",                   # 强制覆盖已有任务
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode == 0:
        print("计划任务安装成功！")
        print(f"可在「任务计划程序」中查看: {计划任务名称}")
    else:
        print(f"安装失败: {result.stderr}")


def 卸载():
    """删除Windows计划任务"""
    print(f"正在卸载计划任务: {计划任务名称}")

    cmd = ["schtasks", "/delete", "/tn", 计划任务名称, "/f"]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode == 0:
        print("计划任务已卸载")
    else:
        print(f"卸载失败: {result.stderr}")


def 打印用法():
    print("用法:")
    print("  python 自动任务.py --安装    注册每日自动更新任务")
    print("  python 自动任务.py --卸载    删除自动更新任务")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        打印用法()
        sys.exit(1)

    参数 = sys.argv[1]
    if 参数 == "--安装":
        安装()
    elif 参数 == "--卸载":
        卸载()
    else:
        print(f"未知参数: {参数}")
        打印用法()
        sys.exit(1)
