import numpy as np
import pandas as pd

START_DATE = pd.Timestamp('2024-01-01')
SPLIT_DATE = pd.Timestamp('2019-01-01')
FRACTAL_N = 3
MIN_MULTIPLE = 2.5
RATIO_LOW = 1.45
RATIO_HIGH = 1.55
WEEKLY_RULE = 'W-FRI'
MONTHLY_RULE = 'ME'
PRE_WINDOW = 32
POST_WINDOW = 64


def is_bottom(lows, idx, n):
    return lows[idx] < float(np.min(lows[idx-n:idx])) and lows[idx] < float(np.min(lows[idx+1:idx+n+1]))


def is_peak(highs, idx, n):
    return highs[idx] > float(np.max(highs[idx-n:idx])) and highs[idx] > float(np.max(highs[idx+1:idx+n+1]))


def resample_ohlc(g, rule):
    o = (
        g.set_index('date')
        .sort_index()
        .resample(rule, label='right', closed='right')
        .agg({'open':'first','high':'max','low':'min','close':'last'})
        .dropna(subset=['open','high','low','close'])
        .reset_index()
    )
    return o


def count_signals_since(frame, ma_window):
    frame = frame.copy()
    frame['ma'] = frame['close'].rolling(window=ma_window, min_periods=ma_window).mean()
    if np.isnan(frame['ma'].iloc[-1]):
        return 0

    lows = frame['low'].to_numpy(dtype=float)
    highs = frame['high'].to_numpy(dtype=float)
    ma = frame['ma'].to_numpy(dtype=float)
    dates = pd.to_datetime(frame['date']).to_numpy(dtype='datetime64[ns]')

    n = FRACTAL_N
    if len(frame) < 2 * n + 1:
        return 0

    bottoms = []
    active_peak_idx = None
    rolling_low = None
    signal_count = 0

    for t in range(len(frame)):
        candidate = t - n
        if candidate >= n:
            if is_bottom(lows, candidate, n):
                bottoms.append(candidate)

            if is_peak(highs, candidate, n):
                prior_bottoms = [b for b in bottoms if b < candidate]
                if prior_bottoms:
                    b = prior_bottoms[-1]
                    bottom_price = float(lows[b])
                    peak_price = float(highs[candidate])
                    if bottom_price > 0 and peak_price / bottom_price >= MIN_MULTIPLE:
                        active_peak_idx = candidate
                        rolling_low = None

        if active_peak_idx is None or t <= active_peak_idx:
            continue

        if rolling_low is None or lows[t] < rolling_low:
            rolling_low = float(lows[t])

        if rolling_low is None or rolling_low <= 0 or np.isnan(ma[t]):
            continue

        ratio = float(ma[t]) / rolling_low
        if RATIO_LOW <= ratio <= RATIO_HIGH and pd.Timestamp(dates[t]) >= START_DATE:
            signal_count += 1

    return signal_count


df = pd.read_parquet('全市场历史数据.parquet', columns=['stock_code','date','open','high','low','close'])
df = df.rename(columns={'stock_code':'code'})
df['date'] = pd.to_datetime(df['date'], errors='coerce')
for col in ['open','high','low','close']:
    df[col] = pd.to_numeric(df[col], errors='coerce')
df = df.dropna(subset=['code','date','open','high','low','close'])
df = df.sort_values(['code','date']).reset_index(drop=True)

codes_total = int(df['code'].nunique())
data_max_date = pd.Timestamp(df['date'].max()).date().isoformat()

stock_with_signal = 0
total_signals = 0
weekly_stock_with_signal = 0
weekly_signals = 0
monthly_stock_with_signal = 0
monthly_signals = 0

for code, g in df.groupby('code', sort=False):
    listing_date = g['date'].iloc[0]
    use_monthly = listing_date < SPLIT_DATE
    rule = MONTHLY_RULE if use_monthly else WEEKLY_RULE
    ma_window = PRE_WINDOW if use_monthly else POST_WINDOW

    frame = resample_ohlc(g[['date','open','high','low','close']], rule)
    min_required = max(ma_window, 2 * FRACTAL_N + 1) + 1
    if len(frame) < min_required:
        continue

    cnt = count_signals_since(frame, ma_window)
    if cnt > 0:
        stock_with_signal += 1
        total_signals += cnt
        if use_monthly:
            monthly_stock_with_signal += 1
            monthly_signals += cnt
        else:
            weekly_stock_with_signal += 1
            weekly_signals += cnt

print('DATA_MAX_DATE', data_max_date)
print('CODES_TOTAL', codes_total)
print('SIGNAL_START', START_DATE.date().isoformat())
print('STOCKS_WITH_SIGNAL', stock_with_signal)
print('SIGNAL_COUNT', total_signals)
print('WEEKLY_STOCKS_WITH_SIGNAL', weekly_stock_with_signal)
print('WEEKLY_SIGNAL_COUNT', weekly_signals)
print('MONTHLY_STOCKS_WITH_SIGNAL', monthly_stock_with_signal)
print('MONTHLY_SIGNAL_COUNT', monthly_signals)
