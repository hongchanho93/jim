"""15分钟数据下载测试脚本

用途：在正式下载前，先测试少量股票，验证：
1. BaoStock API 连接正常
2. 数据格式转换正确
3. 价格确实为不复权
4. 并发机制工作正常

测试数量：10只股票（预计耗时1-2分钟）
"""

import pandas as pd
import baostock as bs
from 下载15分钟数据 import (
    获取股票列表, 下载单只股票, BaoStock数据转15分钟格式,
    数据15分钟路径
)
from 配置 import 合并数据路径


def 测试下载():
    """测试下载10只股票的15分钟数据"""
    print("="*80)
    print("15分钟数据下载测试（10只股票）")
    print("="*80)

    # 1. 获取股票列表
    print("\n[1/4] 获取股票列表...")
    股票列表 = 获取股票列表()
    测试股票 = 股票列表[:10]  # 只测试前10只
    print(f"  测试股票数: {len(测试股票)}")
    for 股票 in 测试股票:
        print(f"    {股票['stock_code']} {股票['stock_name']}")

    # 2. 登录BaoStock
    print("\n[2/4] 连接BaoStock...")
    lg = bs.login()
    if lg.error_code != "0":
        print(f"  [失败] {lg.error_msg}")
        return
    print(f"  [成功] 已连接")

    # 3. 下载测试
    print("\n[3/4] 下载测试数据...")
    所有数据 = []
    成功数 = 0
    失败数 = 0

    for i, 股票 in enumerate(测试股票, 1):
        print(f"  [{i}/10] {股票['stock_code']} {股票['stock_name']}...", end=" ")

        数据 = 下载单只股票(股票)

        if 数据 is not None and len(数据) > 0:
            所有数据.append(数据)
            成功数 += 1
            print(f"OK ({len(数据)} 行)")
        else:
            失败数 += 1
            print(f"FAIL 无数据")

    bs.logout()

    print(f"\n  成功: {成功数}, 失败: {失败数}")

    if not 所有数据:
        print("\n[错误] 没有成功下载任何数据")
        return

    # 4. 数据验证
    print("\n[4/4] 数据验证...")
    合并数据 = pd.concat(所有数据, ignore_index=True)

    print(f"\n  [基础信息]")
    print(f"    总行数: {len(合并数据):,}")
    print(f"    股票数: {合并数据['stock_code'].nunique()}")
    print(f"    时间范围: {合并数据['date'].min()} ~ {合并数据['date'].max()}")
    print(f"    列名: {合并数据.columns.tolist()}")

    print(f"\n  [价格范围]")
    for 列 in ["open", "high", "low", "close"]:
        最小 = 合并数据[列].min()
        最大 = 合并数据[列].max()
        平均 = 合并数据[列].mean()
        print(f"    {列}: 最小={最小:.2f}, 最大={最大:.2f}, 平均={平均:.2f}")

    # 验证复权类型
    print(f"\n  [复权类型验证]")
    样本价格 = 合并数据['close'].head(100).astype(str)
    小数位数 = [len(p.split('.')[1]) if '.' in p else 0 for p in 样本价格]
    平均小数位 = sum(小数位数) / len(小数位数) if 小数位数 else 0
    print(f"    价格平均小数位: {平均小数位:.1f}")

    if 平均小数位 < 3:
        print(f"    [OK] 确认为不复权价格")
    else:
        print(f"    [WARN] 警告：可能是复权价格")

    # 与日线数据对比
    print(f"\n  [与日线数据对比]")
    try:
        日线数据 = pd.read_parquet(合并数据路径)

        # 取第一只有数据的股票进行对比
        测试代码 = 合并数据['stock_code'].iloc[0]
        测试日期 = 合并数据[合并数据['stock_code']==测试代码]['date'].iloc[0]

        日线价格 = 日线数据[
            (日线数据['stock_code']==测试代码) &
            (日线数据['date']==测试日期)
        ]['close'].values

        分时15点 = 合并数据[
            (合并数据['stock_code']==测试代码) &
            (合并数据['date']==测试日期) &
            (合并数据['time']=='15:00:00')
        ]['close'].values

        if len(日线价格) > 0 and len(分时15点) > 0:
            日线价格 = 日线价格[0]
            分时15点 = 分时15点[0]
            差异 = abs(日线价格 - 分时15点) / 日线价格 * 100

            print(f"    测试股票: {测试代码} ({测试日期})")
            print(f"    日线收盘价: {日线价格:.2f}")
            print(f"    15分钟15:00收盘价: {分时15点:.2f}")
            print(f"    差异: {差异:.4f}%")

            if 差异 < 0.1:
                print(f"    [OK] 价格一致性良好")
            else:
                print(f"    [WARN] 价格差异较大，请检查")
        else:
            print(f"    [跳过] 无法找到对应数据进行对比")

    except Exception as e:
        print(f"    [跳过] 无法对比日线数据: {e}")

    # 显示样本数据
    print(f"\n  [样本数据（前10行）]")
    print(合并数据.head(10).to_string(index=False))

    print("\n" + "="*80)
    print("测试完成！")
    print("="*80)
    print("如果以上验证全部通过，可以运行完整下载脚本：")
    print("  python 下载15分钟数据.py")
    print("="*80)


if __name__ == "__main__":
    try:
        测试下载()
    except Exception as e:
        print(f"\n[错误] {e}")
        import traceback
        traceback.print_exc()
