import pandas as pd
f = '全市场历史数据.parquet'
df = pd.read_parquet(f, columns=['date','code'])
d = pd.to_datetime(df['date'], errors='coerce')
print(f, d.min(), d.max(), df['code'].nunique(), len(df))
