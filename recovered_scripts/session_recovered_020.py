from pathlib import Path
import pandas as pd
import pyarrow.parquet as pq

p = next(Path('.').glob('*.parquet'))
# pick daily file by schema
for f in Path('.').glob('*.parquet'):
    cols = set(pq.read_schema(f).names)
    if {'stock_code','date','open','high','low','close'}.issubset(cols) and 'time' not in cols:
        p = f
        break
print('file=', p.name)
print('columns=', pq.read_schema(p).names)

df = pd.read_parquet(p).head(20)
print(df.head(3).to_string())

# show turnover-like columns stats on small sample
sample = pd.read_parquet(p, columns=[c for c in pq.read_schema(p).names if c in ['turnover','turn','volume','outstanding_share']]).head(100000)
print('sample cols=', sample.columns.tolist())
for c in sample.columns:
    s = pd.to_numeric(sample[c], errors='coerce')
    print(c, 'min', float(s.min()), 'max', float(s.max()), 'mean', float(s.mean()))
