import pandas as pd
p = 'all_data_baostock_2025.parquet'
df = pd.read_parquet(p)
print(df.columns.tolist())
print(df.head(2).to_string())
print(df.dtypes.astype(str).to_string())
