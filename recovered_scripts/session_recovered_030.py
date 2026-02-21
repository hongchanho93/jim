from pathlib import Path
from typing import Dict, List, Tuple
import numpy as np
import pandas as pd
import pyarrow.parquet as pq

# params
SPLIT_DATE = pd.Timestamp('2019-01-01')
FRACTAL_N = 3
MIN_MULTIPLE = 2.5
RATIO_LOW = 1.45
RATIO_HIGH = 1.55
TARGET_RATIO = 1.50
PRE_WINDOW = 32
POST_WINDOW = 64

MA_WINDOW = 750
RESONANCE_TOL = 0.03
POST_RESONANCE_MONTHS = 3
MATCH_WINDOW_MONTHS = 2

TURNOVER_LOOKBACK_MONTHS = 18
MONTH_TURNOVER_MIN = 1.0
WARMUP_DAYS_15M = 420

TIME_TO_30_END = {
    '09:45:00':'10:00:00','10:00:00':'10:00:00','10:15:00':'10:30:00','10:30:00':'10:30:00',
    '10:45:00':'11:00:00','11:00:00':'11:00:00','11:15:00':'11:30:00','11:30:00':'11:30:00',
    '13:15:00':'13:30:00','13:30:00':'13:30:00','13:45:00':'14:00:00','14:00:00':'14:00:00',
    '14:15:00':'14:30:00','14:30:00':'14:30:00','14:45:00':'15:00:00','15:00:00':'15:00:00',}
TIME_TO_60_END = {
    '09:45:00':'10:30:00','10:00:00':'10:30:00','10:15:00':'10:30:00','10:30:00':'10:30:00',
    '10:45:00':'11:30:00','11:00:00':'11:30:00','11:15:00':'11:30:00','11:30:00':'11:30:00',
    '13:15:00':'14:00:00','13:30:00':'14:00:00','13:45:00':'14:00:00','14:00:00':'14:00:00',
    '14:15:00':'15:00:00','14:30:00':'15:00:00','14:45:00':'15:00:00','15:00:00':'15:00:00',}

def find_files(root: Path):
    d=[];m=[]
    for p in root.rglob('*.parquet'):
        try: cols=set(pq.read_schema(p).names)
        except: continue
        if {'stock_code','date','open','high','low','close','turnover','stock_name'}.issubset(cols): d.append(p)
        if {'stock_code','date','open','high','low','close','time','stock_name'}.issubset(cols): m.append(p)
    return sorted(d,key=lambda x:x.stat().st_size, reverse=True)[0], sorted(m,key=lambda x:x.stat().st_size, reverse=True)[0]

def resample_ohlc(df_code, rule):
    return (df_code.set_index('date').sort_index().resample(rule,label='right',closed='right')
            .agg({'open':'first','high':'max','low':'min','close':'last'}).dropna().reset_index())

def is_bottom(lows, i, n):
    return lows[i] < float(np.min(lows[i-n:i])) and lows[i] < float(np.min(lows[i+1:i+n+1]))

def is_peak(highs, i, n):
    return highs[i] > float(np.max(highs[i-n:i])) and highs[i] > float(np.max(highs[i+1:i+n+1]))

def stage1_dates(frame, ma_window):
    frame=frame.copy(); frame['ma']=frame['close'].rolling(ma_window,min_periods=ma_window).mean()
    n=FRACTAL_N
    if len(frame)<max(ma_window,2*n+1)+1: return []
    lows=frame['low'].to_numpy(float); highs=frame['high'].to_numpy(float); ma=frame['ma'].to_numpy(float); ts=pd.to_datetime(frame['date'])
    bottoms=[]; active_peak=None; rolling_low=None
    out=[]; in_zone=False; best_idx=None; best_dist=float('inf')
    def close_zone():
        nonlocal in_zone,best_idx,best_dist
        if in_zone and best_idx is not None: out.append(best_idx)
        in_zone=False; best_idx=None; best_dist=float('inf')
    for t in range(len(frame)):
        c=t-n
        if c>=n:
            if is_bottom(lows,c,n): bottoms.append(c)
            if is_peak(highs,c,n):
                prior=[b for b in bottoms if b<c]
                if prior:
                    b=prior[-1]; bp=float(lows[b]); pp=float(highs[c])
                    if bp>0 and pp/bp>=MIN_MULTIPLE:
                        close_zone(); active_peak=c; rolling_low=None
        if active_peak is None or t<=active_peak: continue
        if rolling_low is None or lows[t]<rolling_low: rolling_low=float(lows[t])
        if rolling_low is None or rolling_low<=0 or np.isnan(ma[t]): close_zone(); continue
        ratio=float(ma[t])/rolling_low
        if RATIO_LOW<=ratio<=RATIO_HIGH:
            d=abs(ratio-TARGET_RATIO)
            if (not in_zone) or d<best_dist:
                in_zone=True; best_idx=t; best_dist=d
        else:
            close_zone()
    close_zone()
    return sorted({pd.Timestamp(ts.iloc[i].date()) for i in out})

def add_clean_close(df):
    c=['open','high','low','close']
    invalid=df[c].isna().any(axis=1)|(df[c]<=0).any(axis=1)|df['date'].isna()|df['time'].isna()
    o=df.copy(); o['close_clean']=o['close'].where(~invalid,np.nan); return o

def build_bucket_close(df,time_map,col):
    v=df[df['close_clean'].notna()].copy()
    if v.empty: return pd.DataFrame(columns=['date','ts',col])
    v['bucket_end_time']=v['time'].map(time_map); v=v[v['bucket_end_time'].notna()].copy()
    if v.empty: return pd.DataFrame(columns=['date','ts',col])
    v=v.sort_values('ts')
    g=(v.groupby(['date','bucket_end_time'],as_index=False)['close_clean'].last().rename(columns={'close_clean':col}))
    g['ts']=pd.to_datetime(g['date']+' '+g['bucket_end_time'],errors='coerce')
    g=g.dropna(subset=['ts']).sort_values('ts').reset_index(drop=True)
    return g[['date','ts',col]]

def stage2_dates(df,start_ts,end_ts):
    if df.empty: return []
    data=df.copy(); data['ts']=pd.to_datetime(data['date']+' '+data['time'],errors='coerce'); data=data.dropna(subset=['ts']).sort_values('ts').reset_index(drop=True)
    if data.empty: return []
    data=add_clean_close(data)
    data['ma15']=data['close_clean'].rolling(MA_WINDOW,min_periods=MA_WINDOW).mean()
    df15=data[['date','ts','ma15']].copy(); df30=build_bucket_close(data,TIME_TO_30_END,'close30'); df60=build_bucket_close(data,TIME_TO_60_END,'close60')
    if df30.empty or df60.empty: return []
    df30['ma30']=df30['close30'].rolling(MA_WINDOW,min_periods=MA_WINDOW).mean(); df60['ma60']=df60['close60'].rolling(MA_WINDOW,min_periods=MA_WINDOW).mean()
    base=df60[['date','ts','ma60']].sort_values('ts')
    m=pd.merge_asof(base, df15[['date','ts','ma15']].sort_values('ts'), on='ts', by='date', direction='backward')
    m=pd.merge_asof(m, df30[['date','ts','ma30']].sort_values('ts'), on='ts', by='date', direction='backward')
    m=m.dropna(subset=['ma15','ma30','ma60']).copy()
    if m.empty: return []
    m=m[(m['ts']>=start_ts)&(m['ts']<=end_ts+pd.Timedelta(days=1))].copy()
    if m.empty: return []
    mn=m[['ma15','ma30','ma60']].min(axis=1); mx=m[['ma15','ma30','ma60']].max(axis=1)
    m['res']=((mx-mn)/mn)<=RESONANCE_TOL
    m['bull']=(m['ma15']>m['ma30'])&(m['ma30']>m['ma60'])
    bull_ts=m.loc[m['bull'],'ts'].sort_values().to_numpy(dtype='datetime64[ns]')
    if bull_ts.size==0: return []
    out=[]
    for tr in m.loc[m['res'],'ts'].sort_values().to_list():
        deadline=tr+pd.DateOffset(months=POST_RESONANCE_MONTHS)
        pos=np.searchsorted(bull_ts,np.datetime64(tr),side='left')
        if pos<bull_ts.size:
            tb=pd.Timestamp(bull_ts[pos])
            if tb<=deadline: out.append(pd.Timestamp(tb.date()))
    return sorted(set(out))

def within2m(a,b):
    return (b>=a-pd.DateOffset(months=MATCH_WINDOW_MONTHS)) and (b<=a+pd.DateOffset(months=MATCH_WINDOW_MONTHS))

def nearest(a,hits):
    return sorted(hits,key=lambda b:(abs((b-a).days),b))[0]

def month_turn_map(g):
    mo=g['date'].dt.year*12+g['date'].dt.month
    return g.groupby(mo,sort=False)['turnover'].sum().to_dict()

def turnover_ok(mp,b):
    so=b.year*12+b.month
    for m in range(so-TURNOVER_LOOKBACK_MONTHS, so):
        if mp.get(m,0.0)>MONTH_TURNOVER_MIN: return True
    return False

root=Path.cwd().parent
daily_file,m15_file=find_files(root)
daily=pd.read_parquet(daily_file, columns=['stock_code','stock_name','date','open','high','low','close','turnover'])
daily=daily.rename(columns={'stock_code':'code'})
daily['date']=pd.to_datetime(daily['date'],errors='coerce')
for c in ['open','high','low','close','turnover']:
    daily[c]=pd.to_numeric(daily[c],errors='coerce')
daily=daily.dropna(subset=['code','date','open','high','low','close','turnover']).sort_values(['code','date']).reset_index(drop=True)

latest=pd.Timestamp(daily['date'].max()).normalize(); recent_start=latest-pd.DateOffset(months=2); a_start=recent_start-pd.DateOffset(months=2)
warmup_start=(recent_start-pd.Timedelta(days=WARMUP_DAYS_15M)).date().isoformat(); end_str=latest.date().isoformat()

name_map=(daily[['code','date','stock_name']].sort_values(['code','date']).groupby('code',sort=False).tail(1).set_index('code')['stock_name'].to_dict())

a_map={}
for code,g in daily.groupby('code',sort=False):
    list_date=g['date'].iloc[0]; rule='ME' if list_date<SPLIT_DATE else 'W-FRI'; win=PRE_WINDOW if list_date<SPLIT_DATE else POST_WINDOW
    frame=resample_ohlc(g[['date','open','high','low','close']], rule)
    ds=[d for d in stage1_dates(frame,win) if a_start<=d<=latest]
    if ds: a_map[code]=ds

by_code={code:g.sort_values('date').reset_index(drop=True) for code,g in daily.groupby('code',sort=False)}
mt={code:month_turn_map(g) for code,g in by_code.items()}

rows=[]
for i,code in enumerate(sorted(a_map.keys()), start=1):
    m15_tbl=pq.read_table(m15_file, columns=['stock_code','stock_name','date','time','open','high','low','close'], filters=[('stock_code','=',code),('date','>=',warmup_start),('date','<=',end_str)])
    if m15_tbl.num_rows==0: continue
    b_dates=stage2_dates(m15_tbl.to_pandas(), recent_start, latest)
    if not b_dates: continue
    for a in a_map[code]:
        hits=[b for b in b_dates if within2m(a,b)]
        if not hits: continue
        b=nearest(a,hits)
        if recent_start<=b<=latest and turnover_ok(mt[code], b):
            rows.append({'stock_code':code,'stock_name':name_map.get(code,''),'signal_date':b.date().isoformat(),'a_date':a.date().isoformat()})

out=pd.DataFrame(rows)
out=out.drop_duplicates().sort_values(['signal_date','stock_code']).reset_index(drop=True) if not out.empty else out

print('LATEST', latest.date().isoformat())
print('RECENT_START', recent_start.date().isoformat())
print('COUNT', len(out))
print('UNIQUE_STOCKS', out['stock_code'].nunique() if not out.empty else 0)
if not out.empty:
    print(out.to_csv(index=False))
