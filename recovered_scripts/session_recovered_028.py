from pathlib import Path
from typing import Dict, List, Tuple
import numpy as np
import pandas as pd
import pyarrow.parquet as pq

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

TIME_TO_30_END = {
    '09:45:00': '10:00:00','10:00:00': '10:00:00','10:15:00': '10:30:00','10:30:00': '10:30:00',
    '10:45:00': '11:00:00','11:00:00': '11:00:00','11:15:00': '11:30:00','11:30:00': '11:30:00',
    '13:15:00': '13:30:00','13:30:00': '13:30:00','13:45:00': '14:00:00','14:00:00': '14:00:00',
    '14:15:00': '14:30:00','14:30:00': '14:30:00','14:45:00': '15:00:00','15:00:00': '15:00:00',
}
TIME_TO_60_END = {
    '09:45:00': '10:30:00','10:00:00': '10:30:00','10:15:00': '10:30:00','10:30:00': '10:30:00',
    '10:45:00': '11:30:00','11:00:00': '11:30:00','11:15:00': '11:30:00','11:30:00': '11:30:00',
    '13:15:00': '14:00:00','13:30:00': '14:00:00','13:45:00': '14:00:00','14:00:00': '14:00:00',
    '14:15:00': '15:00:00','14:30:00': '15:00:00','14:45:00': '15:00:00','15:00:00': '15:00:00',
}


def find_data_files(root: Path) -> Tuple[Path, Path]:
    daily_candidates = []
    m15_candidates = []
    for p in root.rglob('*.parquet'):
        try:
            cols = set(pq.read_schema(p).names)
        except Exception:
            continue
        if {'stock_code','date','open','high','low','close','time','stock_name'}.issubset(cols):
            m15_candidates.append(p)
        elif {'stock_code','date','open','high','low','close','turnover','stock_name'}.issubset(cols):
            daily_candidates.append(p)
    return sorted(daily_candidates, key=lambda x:x.stat().st_size, reverse=True)[0], sorted(m15_candidates, key=lambda x:x.stat().st_size, reverse=True)[0]


def resample_ohlc(df_code: pd.DataFrame, rule: str) -> pd.DataFrame:
    return (df_code.set_index('date').sort_index().resample(rule, label='right', closed='right')
            .agg({'open':'first','high':'max','low':'min','close':'last'})
            .dropna(subset=['open','high','low','close']).reset_index())


def is_bottom(lows: np.ndarray, idx: int, n: int) -> bool:
    return lows[idx] < float(np.min(lows[idx-n:idx])) and lows[idx] < float(np.min(lows[idx+1:idx+n+1]))


def is_peak(highs: np.ndarray, idx: int, n: int) -> bool:
    return highs[idx] > float(np.max(highs[idx-n:idx])) and highs[idx] > float(np.max(highs[idx+1:idx+n+1]))


def stage1_dates(frame: pd.DataFrame, ma_window: int) -> List[pd.Timestamp]:
    frame = frame.copy(); frame['ma']=frame['close'].rolling(ma_window,min_periods=ma_window).mean()
    n=FRACTAL_N
    if len(frame) < max(ma_window,2*n+1)+1: return []
    lows=frame['low'].to_numpy(float); highs=frame['high'].to_numpy(float); ma=frame['ma'].to_numpy(float); ts=pd.to_datetime(frame['date'])
    bottoms=[]; active_peak_idx=None; rolling_low=None
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
                        close_zone(); active_peak_idx=c; rolling_low=None
        if active_peak_idx is None or t<=active_peak_idx: continue
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
    invalid=df[c].isna().any(axis=1) | (df[c]<=0).any(axis=1) | df['date'].isna() | df['time'].isna()
    out=df.copy(); out['close_clean']=out['close'].where(~invalid, np.nan); return out


def build_bucket_close(df,time_map,col):
    v=df[df['close_clean'].notna()].copy();
    if v.empty: return pd.DataFrame(columns=['date','ts',col])
    v['bucket_end_time']=v['time'].map(time_map); v=v[v['bucket_end_time'].notna()].copy()
    if v.empty: return pd.DataFrame(columns=['date','ts',col])
    v=v.sort_values('ts')
    g=(v.groupby(['date','bucket_end_time'],as_index=False)['close_clean'].last().rename(columns={'close_clean':col}))
    g['ts']=pd.to_datetime(g['date']+' '+g['bucket_end_time'],errors='coerce')
    g=g.dropna(subset=['ts']).sort_values('ts').reset_index(drop=True)
    return g[['date','ts',col]]


def stage2_dates(df, start_ts, end_ts):
    if df.empty: return []
    data=df.copy(); data['ts']=pd.to_datetime(data['date']+' '+data['time'],errors='coerce'); data=data.dropna(subset=['ts']).sort_values('ts').reset_index(drop=True)
    if data.empty: return []
    data=add_clean_close(data)
    data['ma15']=data['close_clean'].rolling(MA_WINDOW,min_periods=MA_WINDOW).mean()
    df15=data[['date','ts','ma15']].copy(); df30=build_bucket_close(data,TIME_TO_30_END,'close30'); df60=build_bucket_close(data,TIME_TO_60_END,'close60')
    if df30.empty or df60.empty: return []
    df30['ma30']=df30['close30'].rolling(MA_WINDOW,min_periods=MA_WINDOW).mean(); df60['ma60']=df60['close60'].rolling(MA_WINDOW,min_periods=MA_WINDOW).mean()
    base=df60[['date','ts','ma60']].sort_values('ts')
    m=pd.merge_asof(base,df15[['date','ts','ma15']].sort_values('ts'),on='ts',by='date',direction='backward')
    m=pd.merge_asof(m,df30[['date','ts','ma30']].sort_values('ts'),on='ts',by='date',direction='backward')
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


def nearest(a, hits):
    return sorted(hits,key=lambda b:(abs((b-a).days),b))[0]


def month_turn_map(g):
    mo=g['date'].dt.year*12+g['date'].dt.month
    return g.groupby(mo,sort=False)['turnover'].sum().to_dict()


def turnover_ok(mp, b):
    so=b.year*12+b.month
    for m in range(so-TURNOVER_LOOKBACK_MONTHS, so):
        if mp.get(m,0.0)>MONTH_TURNOVER_MIN: return True
    return False

root=Path.cwd().parent
daily_file,m15_file=find_data_files(root)
daily=pd.read_parquet(daily_file, columns=['stock_code','stock_name','date','open','high','low','close','turnover'])
daily=daily.rename(columns={'stock_code':'code'})
daily['date']=pd.to_datetime(daily['date'],errors='coerce')
for c in ['open','high','low','close','turnover']:
    daily[c]=pd.to_numeric(daily[c],errors='coerce')
daily=daily.dropna(subset=['code','date','open','high','low','close','turnover']).sort_values(['code','date']).reset_index(drop=True)

latest=pd.Timestamp(daily['date'].max()).normalize()
recent_start=latest-pd.DateOffset(months=2)
lookaround_start=recent_start-pd.DateOffset(months=2)

# stage1
a_rows=[]
for code,g in daily.groupby('code',sort=False):
    list_date=g['date'].iloc[0]
    rule='ME' if list_date<SPLIT_DATE else 'W-FRI'
    win=PRE_WINDOW if list_date<SPLIT_DATE else POST_WINDOW
    frame=resample_ohlc(g[['date','open','high','low','close']], rule)
    dates=stage1_dates(frame, win)
    for d in dates:
        if lookaround_start<=d<=latest:
            a_rows.append((code,d))
a_df=pd.DataFrame(a_rows, columns=['code','a_date'])

# stage2 and match
by_code={code:g.sort_values('date').reset_index(drop=True) for code,g in daily.groupby('code',sort=False)}
mt_map={code:month_turn_map(g) for code,g in by_code.items()}

b_recent_rows=[]
match_no_turn=[]
match_with_turn=[]

codes=sorted(a_df['code'].unique().tolist())
for i,code in enumerate(codes, start=1):
    m15=pq.read_table(m15_file, columns=['stock_code','stock_name','date','time','open','high','low','close'],
                      filters=[('stock_code','=',code),('date','>=',(lookaround_start-pd.DateOffset(months=3)).date().isoformat()),('date','<=',latest.date().isoformat())]).to_pandas()
    b_dates=stage2_dates(m15, lookaround_start-pd.DateOffset(months=3), latest)
    for b in b_dates:
        if recent_start<=b<=latest:
            b_recent_rows.append((code,b))

    if not b_dates:
        continue
    a_dates=sorted(a_df.loc[a_df['code']==code,'a_date'].tolist())
    for a in a_dates:
        hits=[b for b in b_dates if within2m(a,b)]
        if not hits:
            continue
        b=nearest(a,hits)
        if recent_start<=b<=latest:
            match_no_turn.append((code,a,b))
            if turnover_ok(mt_map[code], b):
                match_with_turn.append((code,a,b))

print('LATEST', latest.date().isoformat())
print('RECENT_START', recent_start.date().isoformat())
print('A_RECENT_COUNT', int(((a_df['a_date']>=recent_start)&(a_df['a_date']<=latest)).sum()))
print('A_RECENT_STOCKS', int(a_df[(a_df['a_date']>=recent_start)&(a_df['a_date']<=latest)]['code'].nunique()))

b_df=pd.DataFrame(b_recent_rows, columns=['code','b_date']).drop_duplicates()
print('B_RECENT_COUNT', len(b_df))
print('B_RECENT_STOCKS', b_df['code'].nunique() if not b_df.empty else 0)

m0=pd.DataFrame(match_no_turn, columns=['code','a_date','b_date']).drop_duplicates()
m1=pd.DataFrame(match_with_turn, columns=['code','a_date','b_date']).drop_duplicates()
print('MATCH_RECENT_NO_TURN_COUNT', len(m0))
print('MATCH_RECENT_NO_TURN_STOCKS', m0['code'].nunique() if not m0.empty else 0)
print('MATCH_RECENT_WITH_TURN_COUNT', len(m1))
print('MATCH_RECENT_WITH_TURN_STOCKS', m1['code'].nunique() if not m1.empty else 0)

if not m0.empty:
    print('SAMPLE_NO_TURN_TOP20')
    print(m0.sort_values(['b_date','code']).head(20).to_string(index=False))
if not m1.empty:
    print('SAMPLE_WITH_TURN_TOP20')
    print(m1.sort_values(['b_date','code']).head(20).to_string(index=False))
