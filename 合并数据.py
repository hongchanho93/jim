"""将5182个单独CSV文件合并为一个Parquet文件"""

import pandas as pd
from pathlib import Path
from tqdm import tqdm
from 配置 import 原始CSV目录, 合并数据路径, 输出列名


def 从文件名解析(文件路径: Path) -> tuple[str, str]:
    """从文件名解析股票代码和名称。如 '000001_平安银行.csv' → ('000001', '平安银行')"""
    文件名 = 文件路径.stem  # 去掉.csv
    部分 = 文件名.split("_", 1)
    代码 = 部分[0].strip()
    名称 = 部分[1].strip() if len(部分) > 1 else ""
    return 代码, 名称


def 读取单个CSV(文件路径: Path) -> pd.DataFrame | None:
    """读取单个CSV文件并标准化格式"""
    try:
        df = pd.read_csv(文件路径, dtype=str)
    except Exception as e:
        print(f"  [跳过] 读取失败 {文件路径.name}: {e}")
        return None

    if df.empty or "date" not in df.columns:
        return None

    代码, 名称 = 从文件名解析(文件路径)
    df["stock_code"] = 代码
    df["stock_name"] = 名称

    # 统一日期格式（errors="coerce" 将无法解析的日期设为NaT，然后丢弃）
    df["date"] = pd.to_datetime(df["date"], format="mixed", dayfirst=False, errors="coerce")
    df = df.dropna(subset=["date"])
    if df.empty:
        return None
    df["date"] = df["date"].dt.strftime("%Y-%m-%d")

    # 数值列转换
    数值列 = ["open", "high", "low", "close", "volume", "amount", "outstanding_share", "turnover"]
    for 列 in 数值列:
        if 列 in df.columns:
            df[列] = pd.to_numeric(df[列], errors="coerce")

    # 只保留需要的列（缺失列填NaN）
    for 列 in 输出列名:
        if 列 not in df.columns:
            df[列] = None

    return df[输出列名]


def 合并所有CSV():
    """主函数：合并所有CSV文件为一个Parquet"""
    csv文件列表 = sorted(原始CSV目录.glob("*.csv"))
    总数 = len(csv文件列表)
    print(f"发现 {总数} 个CSV文件，开始合并...")

    所有数据 = []
    失败数 = 0

    for 文件 in tqdm(csv文件列表, desc="合并进度"):
        df = 读取单个CSV(文件)
        if df is not None and not df.empty:
            所有数据.append(df)
        else:
            失败数 += 1

    print(f"\n成功读取 {len(所有数据)} 个文件，失败 {失败数} 个")

    if not 所有数据:
        print("没有数据可合并！")
        return

    print("正在拼接数据...")
    合并结果 = pd.concat(所有数据, ignore_index=True)

    # 去重：同一股票同一天只保留一条
    去重前 = len(合并结果)
    合并结果 = 合并结果.drop_duplicates(subset=["stock_code", "date"], keep="last")
    去重数 = 去重前 - len(合并结果)
    if 去重数 > 0:
        print(f"去除重复记录 {去重数} 条")

    # 排序
    合并结果 = 合并结果.sort_values(["stock_code", "date"]).reset_index(drop=True)

    # 保存Parquet
    合并数据路径.parent.mkdir(parents=True, exist_ok=True)
    合并结果.to_parquet(合并数据路径, engine="pyarrow", compression="snappy", index=False)

    文件大小MB = 合并数据路径.stat().st_size / (1024 * 1024)
    股票数 = 合并结果["stock_code"].nunique()
    print(f"\n合并完成！")
    print(f"  总行数: {len(合并结果):,}")
    print(f"  股票数: {股票数}")
    print(f"  文件大小: {文件大小MB:.1f} MB")
    print(f"  保存位置: {合并数据路径}")


if __name__ == "__main__":
    合并所有CSV()
