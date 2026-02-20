import pandas as pd
f = 'all_data_baostock_extra.parquet'
df = pd.read_parquet(f, columns=['date','code'])
d = pd.to_datetime(df['date'], errors='coerce')
print(f, d.min(), d.max(), df['code'].nunique(), len(df))
