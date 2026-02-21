from pathlib import Path
import pandas as pd

for p in Path('.').glob('*.parquet'):
    df0 = pd.read_parquet(p, engine='pyarrow')
    cols = list(df0.columns)
    date_col = 'date' if 'date' in cols else None
    code_col = 'code' if 'code' in cols else ('stock_code' if 'stock_code' in cols else None)
    if date_col is not None:
        d = pd.to_datetime(df0[date_col], errors='coerce')
        print(p.name, 'rows=', len(df0), 'codes=', df0[code_col].nunique() if code_col else 'NA', 'min=', d.min(), 'max=', d.max(), 'cols=', cols[:8])
    else:
        print(p.name, 'rows=', len(df0), 'no date col', 'cols=', cols[:8])
