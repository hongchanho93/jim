from pathlib import Path
import pandas as pd
import pyarrow.parquet as pq

root = Path(r'D:\CODEX项目\股票数据维护\数据')
parquets = list(root.glob('*.parquet'))
for p in parquets:
    cols = set(pq.read_schema(p).names)
    if {'stock_code','date','open','high','low','close','time'}.issubset(cols):
        df = pd.read_parquet(p, columns=['stock_code']).head(20)
        print('15m_file', p.name, df['stock_code'].astype(str).head(10).tolist())
    elif {'stock_code','date','open','high','low','close'}.issubset(cols):
        df = pd.read_parquet(p, columns=['stock_code']).head(20)
        print('daily_file', p.name, df['stock_code'].astype(str).head(10).tolist())
