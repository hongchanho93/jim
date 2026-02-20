import pandas as pd

# daily sample
fd='D:/CODEX项目/股票数据维护/数据/全市场历史数据.parquet'
dd = pd.read_parquet(fd, columns=['stock_code']).head(20)
print('daily sample:', dd['stock_code'].tolist()[:10])
print('daily unique sample:', dd['stock_code'].dropna().astype(str).head(5).tolist())

# 15m sample
fm='D:/CODEX项目/股票数据维护/数据/全市场15分钟数据.parquet'
dm = pd.read_parquet(fm, columns=['stock_code']).head(20)
print('15m sample:', dm['stock_code'].tolist()[:10])
print('15m unique sample:', dm['stock_code'].dropna().astype(str).head(5).tolist())
