from pathlib import Path
import pandas as pd
import pyarrow.parquet as pq

for p in Path('.').glob('*.parquet'):
    cols = set(pq.read_schema(p).names)
    print('file', p.name, 'cols_head', list(cols)[:8])
    if 'stock_code' in cols:
        df = pd.read_parquet(p, columns=['stock_code']).head(10)
        print(' sample', df['stock_code'].astype(str).tolist())
