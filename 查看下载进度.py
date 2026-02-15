"""实时显示15分钟数据下载进度"""

import json
import time
from pathlib import Path
from 配置15分钟 import 进度文件路径, 临时文件目录

def 显示进度条(当前, 总数, 宽度=50):
    """绘制进度条"""
    百分比 = 当前 / 总数 if 总数 > 0 else 0
    已完成块 = int(宽度 * 百分比)
    进度条 = '=' * 已完成块 + '-' * (宽度 - 已完成块)
    百分比显示 = f"{百分比*100:.1f}%"
    return f"[{进度条}] {百分比显示} ({当前}/{总数})"

def 获取进度():
    """获取当前下载进度"""
    # 方法1: 读取进度文件
    已完成股票数 = 0
    if 进度文件路径.exists():
        try:
            with open(进度文件路径, 'r', encoding='utf-8') as f:
                进度数据 = json.load(f)
                已完成股票数 = 进度数据.get('完成数量', 0)
        except:
            pass

    # 方法2: 统计临时文件数量
    临时批次数 = 0
    if 临时文件目录.exists():
        临时批次数 = len(list(临时文件目录.glob("batch_*.parquet")))

    return 已完成股票数, 临时批次数

def 主循环():
    """主循环：每5秒刷新一次"""
    总股票数 = 5180
    总批次数 = 52

    print("="*70)
    print("15分钟数据下载进度监控")
    print("="*70)
    print("按 Ctrl+C 退出监控（不会影响下载）\n")

    try:
        while True:
            已完成股票, 已完成批次 = 获取进度()

            # 清屏（Windows）
            print("\033[H\033[J", end="")

            print("="*70)
            print(f"下载时间: {time.strftime('%Y-%m-%d %H:%M:%S')}")
            print("="*70)

            print(f"\n股票进度:")
            print(f"  {显示进度条(已完成股票, 总股票数, 60)}")
            print(f"  已完成: {已完成股票} 只")
            print(f"  剩余: {总股票数 - 已完成股票} 只")

            print(f"\n批次进度:")
            print(f"  {显示进度条(已完成批次, 总批次数, 60)}")
            print(f"  已完成: {已完成批次} 批")
            print(f"  剩余: {总批次数 - 已完成批次} 批")

            if 已完成股票 > 0:
                预计剩余分钟 = (总股票数 - 已完成股票) * 1.5 / 60
                print(f"\n预计剩余时间: {预计剩余分钟:.0f} 分钟 ({预计剩余分钟/60:.1f} 小时)")

            if 已完成股票 >= 总股票数:
                print("\n" + "="*70)
                print("下载完成！")
                print("="*70)
                break

            print("\n刷新: 每5秒自动更新...")
            time.sleep(5)

    except KeyboardInterrupt:
        print("\n\n监控已停止（下载仍在后台继续）")

if __name__ == "__main__":
    主循环()
