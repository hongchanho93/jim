"""统一配置文件"""

import os
from pathlib import Path

# ==================== 路径配置 ====================

# 项目根目录（自动定位为当前配置文件所在目录）
项目目录 = Path(__file__).resolve().parent

# 原始CSV文件目录（只读）
# 可通过环境变量 STOCK_RAW_CSV_DIR 覆盖默认路径
原始CSV目录 = Path(os.getenv("STOCK_RAW_CSV_DIR", str(项目目录 / "数据" / "原始CSV")))

# 合并后的Parquet文件
合并数据路径 = 项目目录 / "数据" / "全市场历史数据.parquet"

# 更新日志
更新日志路径 = 项目目录 / "更新日志.txt"

# ==================== 列定义 ====================

# 最终输出的列名和顺序
输出列名 = [
    "stock_code",        # 股票代码（如 000001）
    "stock_name",        # 股票名称（如 平安银行）
    "date",              # 日期 YYYY-MM-DD
    "open",              # 开盘价
    "high",              # 最高价
    "low",               # 最低价
    "close",             # 收盘价
    "volume",            # 成交量
    "amount",            # 成交额（元）
    "outstanding_share", # 流通股本
    "turnover",          # 换手率
]

# ==================== BaoStock 配置 ====================

# BaoStock 查询字段
BAOSTOCK_FIELDS = "date,open,high,low,close,volume,amount,turn"

# 复权方式：1=后复权 2=前复权 3=不复权
复权方式 = "3"

# 频率：d=日线
频率 = "d"

# 日线更新并发进程数（建议 2-6）
日线并发进程数 = 3

# 是否同步更新原始CSV文件（False=仅更新Parquet，速度快10倍）
同步更新CSV = False

# ==================== 计划任务配置 ====================

# 任务名称
计划任务名称 = "股票数据每日更新"

# 每天执行时间（BaoStock 17:30更新，18:00运行留足余量）
执行时间 = "18:00"
