# 股票数据维护项目

## 项目概述

自动维护全市场股票日线数据（不复权 + 换手率），数据源为 BaoStock。

- **数据规模**: 5,170只股票，15,851,494行历史数据
- **存储格式**: Parquet（442.4 MB）
- **数据范围**: 1990-12-19 至今
- **更新方式**: 每天18:00自动更新

## 文件说明

```
├── 配置.py              # 统一配置（路径、复权方式、列定义等）
├── 合并数据.py          # 一次性工具：合并5182个CSV为一个Parquet
├── 智能更新.py          # 核心脚本：检测缺失日期并从BaoStock补齐
├── 自动任务.py          # 安装/卸载Windows计划任务
├── 启动更新.bat         # 计划任务入口
├── 更新日志.txt         # 自动更新的运行日志
└── 数据/
    └── 全市场历史数据.parquet  # 合并后的数据文件
```

## 重要配置

### 数据格式
- **复权方式**: `复权方式 = "3"` (不复权，原始价格)
  - 1 = 后复权
  - 2 = 前复权
  - 3 = 不复权（当前设置）
- **是否同步CSV**: `同步更新CSV = False` (仅更新Parquet，速度快10倍)

### 数据列定义
```
stock_code, stock_name, date, open, high, low, close,
volume, amount, outstanding_share, turnover
```

## 使用说明

### 日常更新
```bash
python 智能更新.py
```
- 自动检测缺失交易日
- 从BaoStock批量补齐
- 同时更新Parquet文件

### 自动任务管理
```bash
python 自动任务.py --安装    # 安装每日18:00自动更新
python 自动任务.py --卸载    # 卸载自动任务
```

### 手动触发计划任务（测试用）
```powershell
Start-ScheduledTask -TaskName "股票数据每日更新"
```

## ⚠️ 重要注意事项

1. **BaoStock互斥**
   - 智能更新脚本内置互斥锁
   - 不要同时运行多个BaoStock脚本（会互相影响速度）
   - 如需强制运行，删除 `C:\Users\Administrator\AppData\Local\Temp\baostock.lock`

2. **数据完整性**
   - 原始CSV文件在 `D:\Quant\Data\stock_data_sina_raw\` (只读，不会修改)
   - 主数据文件：`数据/全市场历史数据.parquet`
   - 默认不同步更新CSV以提升速度

3. **复权数据切换**
   - 如需复权数据，修改 `配置.py` 中 `复权方式 = "2"` (前复权)
   - 重新运行 `智能更新.py` 即可

4. **性能优化**
   - 只更新缺失数据的股票（智能跳过已是最新的）
   - 默认不更新今日数据（避免盘中不完整数据）
   - 只更新到最近一个完整交易日

## 数据验证

```python
import pandas as pd
df = pd.read_parquet('数据/全市场历史数据.parquet')
print(f"总行数: {len(df):,}")
print(f"股票数: {df['stock_code'].nunique()}")
print(f"日期范围: {df['date'].min()} ~ {df['date'].max()}")
print(f"是否有换手率: {'turnover' in df.columns}")
```

## 故障排查

### 更新失败
1. 查看 `更新日志.txt`
2. 检查网络连接
3. 确认BaoStock服务是否正常

### 计划任务未运行
1. 打开「任务计划程序」查看任务状态
2. 检查 `更新日志.txt` 最后运行时间
3. 手动触发测试：`Start-ScheduledTask -TaskName "股票数据每日更新"`
