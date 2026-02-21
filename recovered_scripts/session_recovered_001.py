import pandas as pd
from pathlib import Path
p = Path(r'D:\CODEX项目\2.5倍超跌反弹策略\all_data_baostock_2025.parquet')
df = pd.read_parquet(p)
print(df.columns.tolist())
print(df.head(2).to_string())
print(df.dtypes.astype(str).to_string())
