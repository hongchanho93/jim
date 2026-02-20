import pandas as pd
for f in ['all_data_baostock_2025.parquet','all_data_baostock_extra.parquet']:
    df = pd.read_parquet(f, columns=['date','code'])
    d = pd.to_datetime(df['date'], errors='coerce')
    print(f, d.min(), d.max(), df['code'].nunique(), len(df))
