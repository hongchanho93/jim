"""智能检测缺失交易日并用BaoStock补齐数据"""

import sys
import datetime
import time
from pathlib import Path
import pandas as pd
import baostock as bs
from tqdm import tqdm
from 配置 import (
    原始CSV目录, 合并数据路径, 输出列名,
    BAOSTOCK_FIELDS, 复权方式, 频率, 同步更新CSV,
)

# BaoStock互斥锁文件
BAOSTOCK_LOCK = Path(r"C:\Users\Administrator\AppData\Local\Temp\baostock.lock")


def 代码转BaoStock格式(stock_code: str) -> str:
    """将纯数字代码转为BaoStock格式。'600000' → 'sh.600000', '000001' → 'sz.000001'"""
    if stock_code.startswith(("6", "9")):
        return f"sh.{stock_code}"
    else:
        return f"sz.{stock_code}"


def 获取交易日历(开始日期: str, 结束日期: str) -> set[str]:
    """通过BaoStock获取指定区间内的交易日集合"""
    rs = bs.query_trade_dates(start_date=开始日期, end_date=结束日期)
    交易日集合 = set()
    while rs.next():
        row = rs.get_row_data()
        # row[0]=日期, row[1]=是否交易日(1/0)
        if row[1] == "1":
            交易日集合.add(row[0])
    return 交易日集合


def 查询BaoStock数据(bs代码: str, 开始日期: str, 结束日期: str) -> pd.DataFrame | None:
    """从BaoStock拉取单只股票的历史K线数据"""
    rs = bs.query_history_k_data_plus(
        code=bs代码,
        fields=BAOSTOCK_FIELDS,
        start_date=开始日期,
        end_date=结束日期,
        frequency=频率,
        adjustflag=复权方式,
    )
    if rs.error_code != "0":
        return None

    rows = []
    while rs.next():
        rows.append(rs.get_row_data())

    if not rows:
        return None

    df = pd.DataFrame(rows, columns=rs.fields)
    return df


def BaoStock数据转标准格式(df: pd.DataFrame, stock_code: str, stock_name: str) -> pd.DataFrame:
    """将BaoStock返回的数据转换为项目标准格式"""
    结果 = pd.DataFrame()
    结果["stock_code"] = stock_code
    结果["stock_name"] = stock_name
    结果["date"] = df["date"]
    结果["open"] = pd.to_numeric(df["open"], errors="coerce")
    结果["high"] = pd.to_numeric(df["high"], errors="coerce")
    结果["low"] = pd.to_numeric(df["low"], errors="coerce")
    结果["close"] = pd.to_numeric(df["close"], errors="coerce")
    结果["volume"] = pd.to_numeric(df["volume"], errors="coerce")
    结果["amount"] = pd.to_numeric(df["amount"], errors="coerce")

    # 换手率
    turn = pd.to_numeric(df["turn"], errors="coerce")
    结果["turnover"] = turn

    # 流通股本 = 成交量 / 换手率 * 100（换手率是百分比）
    结果["outstanding_share"] = 结果.apply(
        lambda r: r["volume"] / r["turnover"] * 100
        if pd.notna(r["turnover"]) and r["turnover"] > 0
        else 0,
        axis=1,
    )

    return 结果[输出列名]


def 追加到CSV(新数据: pd.DataFrame, stock_code: str, stock_name: str):
    """将新数据追加到对应的原始CSV文件"""
    # 查找匹配的CSV文件
    匹配文件 = list(原始CSV目录.glob(f"{stock_code}_*.csv"))
    if not 匹配文件:
        # 新股票，创建新文件
        文件路径 = 原始CSV目录 / f"{stock_code}_{stock_name}.csv"
        csv列 = ["open", "high", "low", "close", "volume", "amount", "outstanding_share", "turnover", "date"]
        新数据[csv列].to_csv(文件路径, index=False)
        return

    文件路径 = 匹配文件[0]
    csv列 = ["open", "high", "low", "close", "volume", "amount", "outstanding_share", "turnover", "date"]
    新数据[csv列].to_csv(文件路径, mode="a", header=False, index=False)


def 安全读取Parquet() -> pd.DataFrame:
    """读取Parquet文件，处理可能的损坏"""
    if not 合并数据路径.exists():
        print(f"[错误] Parquet文件不存在: {合并数据路径}")
        print("请先运行 合并数据.py")
        sys.exit(1)

    try:
        return pd.read_parquet(合并数据路径)
    except Exception as e:
        print(f"[错误] 读取Parquet失败: {e}")
        sys.exit(1)


def 检查BaoStock锁():
    """检查是否有其他BaoStock脚本正在运行"""
    if BAOSTOCK_LOCK.exists():
        # 检查锁文件是否超时（超过30分钟认为是僵尸锁）
        锁文件时间 = BAOSTOCK_LOCK.stat().st_mtime
        if time.time() - 锁文件时间 < 1800:  # 30分钟
            return True
        else:
            # 清理僵尸锁
            BAOSTOCK_LOCK.unlink()
    return False


def 创建BaoStock锁():
    """创建锁文件"""
    BAOSTOCK_LOCK.parent.mkdir(parents=True, exist_ok=True)
    BAOSTOCK_LOCK.write_text(f"股票数据更新 - {datetime.datetime.now()}")


def 释放BaoStock锁():
    """释放锁文件"""
    if BAOSTOCK_LOCK.exists():
        BAOSTOCK_LOCK.unlink()


def 智能更新():
    """主函数：检测缺失数据并补齐"""
    print(f"{'='*60}")
    print(f"股票数据智能更新 - {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}")

    # 检查是否有其他BaoStock脚本在运行
    if 检查BaoStock锁():
        print("\n[警告] 检测到其他BaoStock脚本正在运行！")
        print("为避免速度互相影响，建议等待其他脚本完成后再运行。")
        print("如需强制运行，请手动删除锁文件：")
        print(f"  {BAOSTOCK_LOCK}")
        return

    创建BaoStock锁()

    # 1. 读取现有数据
    print("\n[1/4] 读取现有数据...")
    现有数据 = 安全读取Parquet()
    print(f"  现有数据: {len(现有数据):,} 行, {现有数据['stock_code'].nunique()} 只股票")

    # 2. 分析每只股票的最新日期
    print("\n[2/4] 分析各股票数据情况...")
    股票最新日期 = 现有数据.groupby(["stock_code", "stock_name"])["date"].max().reset_index()
    股票最新日期.columns = ["stock_code", "stock_name", "last_date"]

    # 3. 登录BaoStock并获取交易日历
    print("\n[3/4] 连接BaoStock...")
    lg = bs.login()
    if lg.error_code != "0":
        print(f"  BaoStock登录失败: {lg.error_msg}")
        return
    print("  BaoStock登录成功")

    # 获取交易日历：从最早的last_date到今天
    今天 = datetime.date.today().strftime("%Y-%m-%d")
    最早日期 = 股票最新日期["last_date"].min()
    全部交易日 = 获取交易日历(最早日期, 今天)
    print(f"  交易日历范围: {最早日期} ~ {今天}, 共 {len(全部交易日)} 个交易日")

    # 预筛选：找出真正缺失交易日的股票
    # 获取最近一个交易日（排除今天，因为今天可能还未收盘）
    全部交易日列表 = sorted(全部交易日)
    最近交易日 = 全部交易日列表[-1] if 全部交易日列表[-1] != 今天 else (全部交易日列表[-2] if len(全部交易日列表) > 1 else 今天)

    需要更新的股票 = 股票最新日期[股票最新日期["last_date"] < 最近交易日].copy()
    print(f"  最近交易日: {最近交易日}")
    print(f"  需要更新: {len(需要更新的股票)} 只（已是最新: {len(股票最新日期) - len(需要更新的股票)} 只）")

    if len(需要更新的股票) == 0:
        print("\n所有股票数据都已是最新！")
        bs.logout()
        return

    # 4. 逐只股票检查并更新
    print(f"\n[4/4] 开始更新...")
    更新股票数 = 0
    新增行数 = 0
    跳过数 = 0
    失败列表 = []
    所有新数据 = []
    CSV批量更新 = {}  # 批量累积CSV更新，最后统一写入

    for _, row in tqdm(需要更新的股票.iterrows(), total=len(需要更新的股票), desc="更新进度"):
        代码 = row["stock_code"]
        名称 = row["stock_name"]
        最后日期 = row["last_date"]

        # 计算缺失的交易日（不包括今天，避免盘中数据不完整）
        缺失交易日 = {d for d in 全部交易日 if 最后日期 < d <= 最近交易日}

        if not 缺失交易日:
            跳过数 += 1
            continue

        # 确定查询区间（到最近交易日，不查今天）
        开始 = min(缺失交易日)
        结束 = 最近交易日
        bs代码 = 代码转BaoStock格式(代码)

        try:
            新数据 = 查询BaoStock数据(bs代码, 开始, 结束)
            if 新数据 is None or 新数据.empty:
                跳过数 += 1
                continue

            标准数据 = BaoStock数据转标准格式(新数据, 代码, 名称)

            # 过滤掉已存在的日期
            标准数据 = 标准数据[标准数据["date"] > 最后日期]
            if 标准数据.empty:
                跳过数 += 1
                continue

            所有新数据.append(标准数据)

            # 累积CSV更新（稍后批量写入）
            if 同步更新CSV:
                if 代码 not in CSV批量更新:
                    CSV批量更新[代码] = {"名称": 名称, "数据": []}
                CSV批量更新[代码]["数据"].append(标准数据)

            更新股票数 += 1
            新增行数 += len(标准数据)

        except Exception as e:
            失败列表.append((代码, 名称, str(e)))

    bs.logout()

    # 批量写入CSV（减少磁盘IO）- 可选
    if 同步更新CSV and CSV批量更新:
        print(f"\n正在更新 {len(CSV批量更新)} 个CSV文件...")
        for 代码, 信息 in CSV批量更新.items():
            名称 = 信息["名称"]
            合并数据 = pd.concat(信息["数据"], ignore_index=True)
            追加到CSV(合并数据, 代码, 名称)

    # 5. 更新Parquet文件
    if 所有新数据:
        print("\n正在更新Parquet文件...")
        新增合并 = pd.concat(所有新数据, ignore_index=True)
        更新后 = pd.concat([现有数据, 新增合并], ignore_index=True)
        更新后 = 更新后.drop_duplicates(subset=["stock_code", "date"], keep="last")
        更新后 = 更新后.sort_values(["stock_code", "date"]).reset_index(drop=True)
        更新后.to_parquet(合并数据路径, engine="pyarrow", compression="snappy", index=False)

    # 6. 打印摘要
    print(f"\n{'='*60}")
    print(f"更新完成！")
    print(f"  更新股票: {更新股票数} 只")
    print(f"  新增数据: {新增行数:,} 行")
    print(f"  无需更新: {跳过数} 只")
    print(f"  更新失败: {len(失败列表)} 只")

    if 失败列表:
        print(f"\n失败详情:")
        for 代码, 名称, 原因 in 失败列表[:20]:
            print(f"  {代码} {名称}: {原因}")
        if len(失败列表) > 20:
            print(f"  ...还有 {len(失败列表) - 20} 只")

    print(f"{'='*60}")


if __name__ == "__main__":
    智能更新()
