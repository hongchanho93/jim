from pathlib import Path
import pandas as pd
for p in Path('.').glob('*.parquet'):
    df = pd.read_parquet(p, columns=['date','code'])
    d = pd.to_datetime(df['date'], errors='coerce')
    print(p.name, d.min(), d.max(), df['code'].nunique(), len(df))
