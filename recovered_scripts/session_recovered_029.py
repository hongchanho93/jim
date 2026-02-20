from pathlib import Path
from typing import Dict, List, Tuple
import numpy as np
import pandas as pd
import pyarrow.parquet as pq

RESONANCE_TOL=0.03
MA_WINDOW=750
TIME_TO_30_END={
    '09:45:00':'10:00:00','10:00:00':'10:00:00','10:15:00':'10:30:00','10:30:00':'10:30:00',
    '10:45:00':'11:00:00','11:00:00':'11:00:00','11:15:00':'11:30:00','11:30:00':'11:30:00',
    '13:15:00':'13:30:00','13:30:00':'13:30:00','13:45:00':'14:00:00','14:00:00':'14:00:00',
    '14:15:00':'14:30:00','14:30:00':'14:30:00','14:45:00':'15:00:00','15:00:00':'15:00:00',}
TIME_TO_60_END={
    '09:45:00':'10:30:00','10:00:00':'10:30:00','10:15:00':'10:30:00','10:30:00':'10:30:00',
    '10:45:00':'11:30:00','11:00:00':'11:30:00','11:15:00':'11:30:00','11:30:00':'11:30:00',
    '13:15:00':'14:00:00','13:30:00':'14:00:00','13:45:00':'14:00:00','14:00:00':'14:00:00',
    '14:15:00':'15:00:00','14:30:00':'15:00:00','14:45:00':'15:00:00','15:00:00':'15:00:00',}

def find_files(root: Path):
    d=None;m=None
    for p in root.rglob('*.parquet'):
        try: cols=set(pq.read_schema(p).names)
        except: continue
        if {'stock_code','date','open','high','low','close','time','stock_name'}.issubset(cols): m=p
        elif {'stock_code','date','open','high','low','close','turnover','stock_name'}.issubset(cols): d=p
    return d,m

def add_clean_close(df):
    c=['open','high','low','close']
    invalid=df[c].isna().any(axis=1)|(df[c]<=0).any(axis=1)|df['date'].isna()|df['time'].isna()
    o=df.copy(); o['close_clean']=o['close'].where(~invalid,np.nan); return o

def build_bucket_close(df_15m,time_map,col):
    v=df_15m[df_15m['close_clean'].notna()].copy()
    if v.empty: return pd.DataFrame(columns=['date','ts',col])
    v['bucket_end_time']=v['time'].map(time_map); v=v[v['bucket_end_time'].notna()].copy()
    if v.empty: return pd.DataFrame(columns=['date','ts',col])
    v=v.sort_values('ts')
    g=(v.groupby(['date','bucket_end_time'],as_index=False)['close_clean'].last().rename(columns={'close_clean':col}))
    g['ts']=pd.to_datetime(g['date']+' '+g['bucket_end_time'],errors='coerce')
    g=g.dropna(subset=['ts']).sort_values('ts').reset_index(drop=True)
    return g[['date','ts',col]]

def merged_stage2(df):
    if df.empty: return pd.DataFrame()
    data=df.copy(); data['ts']=pd.to_datetime(data['date']+' '+data['time'],errors='coerce'); data=data.dropna(subset=['ts']).sort_values('ts').reset_index(drop=True)
    if data.empty: return pd.DataFrame()
    data=add_clean_close(data)
    data['ma15']=data['close_clean'].rolling(MA_WINDOW,min_periods=MA_WINDOW).mean()
    df15=data[['date','ts','ma15']].copy(); df30=build_bucket_close(data,TIME_TO_30_END,'close30'); df60=build_bucket_close(data,TIME_TO_60_END,'close60')
    if df30.empty or df60.empty: return pd.DataFrame()
    df30['ma30']=df30['close30'].rolling(MA_WINDOW,min_periods=MA_WINDOW).mean(); df60['ma60']=df60['close60'].rolling(MA_WINDOW,min_periods=MA_WINDOW).mean()
    base=df60[['date','ts','ma60']].sort_values('ts')
    m=pd.merge_asof(base, df15[['date','ts','ma15']].sort_values('ts'), on='ts', by='date', direction='backward')
    m=pd.merge_asof(m, df30[['date','ts','ma30']].sort_values('ts'), on='ts', by='date', direction='backward')
    m=m.dropna(subset=['ma15','ma30','ma60']).copy()
    if m.empty: return m
    mn=m[['ma15','ma30','ma60']].min(axis=1); mx=m[['ma15','ma30','ma60']].max(axis=1)
    m['res']=((mx-mn)/mn)<=RESONANCE_TOL
    m['bull']=(m['ma15']>m['ma30'])&(m['ma30']>m['ma60'])
    return m

root=Path.cwd().parent
daily,m15=find_files(root)
df_daily=pd.read_parquet(daily, columns=['stock_code','date'])
df_daily['date']=pd.to_datetime(df_daily['date'],errors='coerce')
latest=pd.Timestamp(df_daily['date'].max()).normalize(); recent_start=latest-pd.DateOffset(months=2)

codes=sorted(df_daily['stock_code'].dropna().astype(str).unique().tolist())

res_rows=0; bull_rows=0; both_rows=0; code_res=0; code_bull=0; code_both=0
samples=[]
for i,code in enumerate(codes, start=1):
    t=pq.read_table(m15, columns=['stock_code','date','time','open','high','low','close'], filters=[('stock_code','=',code),('date','>=',recent_start.date().isoformat()),('date','<=',latest.date().isoformat())])
    if t.num_rows==0: continue
    g=t.to_pandas()
    mrg=merged_stage2(g)
    if mrg.empty: continue
    mrg=mrg[(mrg['ts']>=recent_start)&(mrg['ts']<=latest+pd.Timedelta(days=1))]
    if mrg.empty: continue
    r=int(mrg['res'].sum()); b=int(mrg['bull'].sum()); rb=int((mrg['res']&mrg['bull']).sum())
    res_rows+=r; bull_rows+=b; both_rows+=rb
    if r>0: code_res+=1
    if b>0: code_bull+=1
    if rb>0:
        code_both+=1
        if len(samples)<20:
            samples.append(code)

print('LATEST', latest.date().isoformat())
print('RECENT_START', recent_start.date().isoformat())
print('CODES_SCANNED', len(codes))
print('RES_ROWS_RECENT', res_rows)
print('BULL_ROWS_RECENT', bull_rows)
print('RES_AND_BULL_SAME_BAR_ROWS_RECENT', both_rows)
print('RES_CODES_RECENT', code_res)
print('BULL_CODES_RECENT', code_bull)
print('RES_AND_BULL_SAME_BAR_CODES_RECENT', code_both)
print('SAMPLE_CODES_RES_AND_BULL_SAME_BAR', ';'.join(samples))
