from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

START_2025 = pd.Timestamp('2025-01-01')
END_2025 = pd.Timestamp('2025-12-31')
LOOKAROUND_START = pd.Timestamp('2024-11-01')
LOOKAROUND_END = pd.Timestamp('2025-12-31')

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

TIME_TO_30_END = {
    '09:45:00': '10:00:00', '10:00:00': '10:00:00', '10:15:00': '10:30:00', '10:30:00': '10:30:00',
    '10:45:00': '11:00:00', '11:00:00': '11:00:00', '11:15:00': '11:30:00', '11:30:00': '11:30:00',
    '13:15:00': '13:30:00', '13:30:00': '13:30:00', '13:45:00': '14:00:00', '14:00:00': '14:00:00',
    '14:15:00': '14:30:00', '14:30:00': '14:30:00', '14:45:00': '15:00:00', '15:00:00': '15:00:00',
}
TIME_TO_60_END = {
    '09:45:00': '10:30:00', '10:00:00': '10:30:00', '10:15:00': '10:30:00', '10:30:00': '10:30:00',
    '10:45:00': '11:30:00', '11:00:00': '11:30:00', '11:15:00': '11:30:00', '11:30:00': '11:30:00',
    '13:15:00': '14:00:00', '13:30:00': '14:00:00', '13:45:00': '14:00:00', '14:00:00': '14:00:00',
    '14:15:00': '15:00:00', '14:30:00': '15:00:00', '14:45:00': '15:00:00', '15:00:00': '15:00:00',
}


def pick_daily_15m_files(data_dir: Path):
    daily_file = None
    m15_file = None
    for p in data_dir.glob('*.parquet'):
        cols = set(pq.read_schema(p).names)
        if {'stock_code', 'date', 'open', 'high', 'low', 'close', 'time'}.issubset(cols):
            m15_file = p
        elif {'stock_code', 'date', 'open', 'high', 'low', 'close'}.issubset(cols):
            daily_file = p
    if daily_file is None or m15_file is None:
        raise RuntimeError('Cannot locate daily/15m parquet files')
    return daily_file, m15_file


def resample_ohlc(df_code: pd.DataFrame, rule: str) -> pd.DataFrame:
    out = (
        df_code.set_index('date')
        .sort_index()
        .resample(rule, label='right', closed='right')
        .agg({'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last'})
        .dropna(subset=['open', 'high', 'low', 'close'])
        .reset_index()
    )
    return out


def is_bottom(lows: np.ndarray, idx: int, n: int) -> bool:
    return lows[idx] < float(np.min(lows[idx - n:idx])) and lows[idx] < float(np.min(lows[idx + 1:idx + n + 1]))


def is_peak(highs: np.ndarray, idx: int, n: int) -> bool:
    return highs[idx] > float(np.max(highs[idx - n:idx])) and highs[idx] > float(np.max(highs[idx + 1:idx + n + 1]))


def stage1_signal_dates(frame: pd.DataFrame, ma_window: int) -> List[pd.Timestamp]:
    frame = frame.copy()
    frame['ma'] = frame['close'].rolling(window=ma_window, min_periods=ma_window).mean()
    n = FRACTAL_N
    if len(frame) < max(ma_window, 2 * n + 1) + 1:
        return []

    lows = frame['low'].to_numpy(dtype=float)
    highs = frame['high'].to_numpy(dtype=float)
    ma = frame['ma'].to_numpy(dtype=float)
    ts = pd.to_datetime(frame['date'])

    bottoms: List[int] = []
    active_peak_idx = None
    rolling_low = None

    all_signal_idx: List[int] = []
    in_zone = False
    zone_best_idx = None
    zone_best_dist = float('inf')

    def close_zone():
        nonlocal in_zone, zone_best_idx, zone_best_dist
        if in_zone and zone_best_idx is not None:
            all_signal_idx.append(zone_best_idx)
        in_zone = False
        zone_best_idx = None
        zone_best_dist = float('inf')

    for t in range(len(frame)):
        candidate = t - n
        if candidate >= n:
            if is_bottom(lows, candidate, n):
                bottoms.append(candidate)
            if is_peak(highs, candidate, n):
                prior_bottoms = [b for b in bottoms if b < candidate]
                if prior_bottoms:
                    b = prior_bottoms[-1]
                    bp = float(lows[b])
                    pp = float(highs[candidate])
                    if bp > 0 and pp / bp >= MIN_MULTIPLE:
                        close_zone()
                        active_peak_idx = candidate
                        rolling_low = None

        if active_peak_idx is None or t <= active_peak_idx:
            continue

        if rolling_low is None or lows[t] < rolling_low:
            rolling_low = float(lows[t])

        if rolling_low is None or rolling_low <= 0 or np.isnan(ma[t]):
            close_zone()
            continue

        ratio = float(ma[t]) / rolling_low
        in_band = RATIO_LOW <= ratio <= RATIO_HIGH
        if in_band:
            dist = abs(ratio - TARGET_RATIO)
            if not in_zone:
                in_zone = True
                zone_best_idx = t
                zone_best_dist = dist
            elif dist < zone_best_dist:
                zone_best_idx = t
                zone_best_dist = dist
        else:
            close_zone()

    close_zone()
    if not all_signal_idx:
        return []

    out_dates = sorted({pd.Timestamp(ts.iloc[i].date()) for i in all_signal_idx})
    return out_dates


def add_clean_close(df: pd.DataFrame) -> pd.DataFrame:
    c = ['open', 'high', 'low', 'close']
    invalid = df[c].isna().any(axis=1)
    invalid |= (df[c] <= 0).any(axis=1)
    invalid |= df['date'].isna() | df['time'].isna()
    out = df.copy()
    out['close_clean'] = out['close'].where(~invalid, np.nan)
    return out


def build_bucket_close(df_15m: pd.DataFrame, time_map: Dict[str, str], close_col_name: str) -> pd.DataFrame:
    valid = df_15m[df_15m['close_clean'].notna()].copy()
    if valid.empty:
        return pd.DataFrame(columns=['date', 'ts', close_col_name])
    valid['bucket_end_time'] = valid['time'].map(time_map)
    valid = valid[valid['bucket_end_time'].notna()].copy()
    if valid.empty:
        return pd.DataFrame(columns=['date', 'ts', close_col_name])
    valid = valid.sort_values('ts')
    grouped = (
        valid.groupby(['date', 'bucket_end_time'], as_index=False)['close_clean']
        .last()
        .rename(columns={'close_clean': close_col_name})
    )
    grouped['ts'] = pd.to_datetime(grouped['date'] + ' ' + grouped['bucket_end_time'], errors='coerce')
    grouped = grouped.dropna(subset=['ts']).sort_values('ts').reset_index(drop=True)
    return grouped[['date', 'ts', close_col_name]]


def stage2_signal_dates_from_15m(df: pd.DataFrame) -> List[pd.Timestamp]:
    if df.empty:
        return []
    data = df.copy()
    data['ts'] = pd.to_datetime(data['date'] + ' ' + data['time'], errors='coerce')
    data = data.dropna(subset=['ts']).sort_values('ts').reset_index(drop=True)
    if data.empty:
        return []

    data = add_clean_close(data)
    data['ma15'] = data['close_clean'].rolling(window=MA_WINDOW, min_periods=MA_WINDOW).mean()

    df15 = data[['date', 'ts', 'ma15']].copy()
    df30 = build_bucket_close(data, TIME_TO_30_END, 'close30')
    df60 = build_bucket_close(data, TIME_TO_60_END, 'close60')
    if df30.empty or df60.empty:
        return []

    df30['ma30'] = df30['close30'].rolling(window=MA_WINDOW, min_periods=MA_WINDOW).mean()
    df60['ma60'] = df60['close60'].rolling(window=MA_WINDOW, min_periods=MA_WINDOW).mean()

    base = df60[['date', 'ts', 'ma60']].sort_values('ts')
    right15 = df15[['date', 'ts', 'ma15']].sort_values('ts')
    right30 = df30[['date', 'ts', 'ma30']].sort_values('ts')

    merged = pd.merge_asof(base, right15, on='ts', by='date', direction='backward')
    merged = pd.merge_asof(merged, right30, on='ts', by='date', direction='backward')
    merged = merged.dropna(subset=['ma15', 'ma30', 'ma60']).copy()
    if merged.empty:
        return []

    merged = merged[(merged['ts'] >= LOOKAROUND_START) & (merged['ts'] <= (END_2025 + pd.Timedelta(days=1)))].copy()
    if merged.empty:
        return []

    ma_block = merged[['ma15', 'ma30', 'ma60']]
    ma_min = ma_block.min(axis=1)
    ma_max = ma_block.max(axis=1)
    merged['spread'] = (ma_max - ma_min) / ma_min
    merged['res_hit'] = merged['spread'] <= RESONANCE_TOL
    merged['bull_hit'] = (merged['ma15'] > merged['ma30']) & (merged['ma30'] > merged['ma60'])

    bull_ts = merged.loc[merged['bull_hit'], 'ts'].sort_values().to_numpy(dtype='datetime64[ns]')
    if bull_ts.size == 0:
        return []

    b_dates: List[pd.Timestamp] = []
    for ts_r in merged.loc[merged['res_hit'], 'ts'].sort_values().to_list():
        deadline = ts_r + pd.DateOffset(months=POST_RESONANCE_MONTHS)
        pos = np.searchsorted(bull_ts, np.datetime64(ts_r), side='left')
        if pos < bull_ts.size:
            ts_b = pd.Timestamp(bull_ts[pos])
            if ts_b <= deadline:
                b_dates.append(pd.Timestamp(ts_b.date()))

    if not b_dates:
        return []
    return sorted(set(b_dates))


def within_two_months(a: pd.Timestamp, b: pd.Timestamp) -> bool:
    return (b >= a - pd.DateOffset(months=MATCH_WINDOW_MONTHS)) and (b <= a + pd.DateOffset(months=MATCH_WINDOW_MONTHS))


def load_15m_stock(data15: Path, code: str) -> pd.DataFrame:
    table = pq.read_table(
        data15,
        columns=['stock_code', 'stock_name', 'date', 'time', 'open', 'high', 'low', 'close'],
        filters=[('stock_code', '=', code), ('date', '>=', '2024-01-01'), ('date', '<=', '2025-12-31')],
    )
    if table.num_rows == 0:
        return pd.DataFrame()
    return table.to_pandas()


def main():
    data_dir = Path(r'D:\CODEX项目\股票数据维护\数据')
    daily_file, m15_file = pick_daily_15m_files(data_dir)
    print('USING_DAILY', daily_file.name)
    print('USING_15M', m15_file.name)

    daily = pd.read_parquet(daily_file, columns=['stock_code', 'date', 'open', 'high', 'low', 'close'])
    daily = daily.rename(columns={'stock_code': 'code'})
    daily['date'] = pd.to_datetime(daily['date'], errors='coerce')
    for c in ['open', 'high', 'low', 'close']:
        daily[c] = pd.to_numeric(daily[c], errors='coerce')
    daily = daily.dropna(subset=['code', 'date', 'open', 'high', 'low', 'close'])
    daily = daily.sort_values(['code', 'date']).reset_index(drop=True)

    # Stage-1 signals in lookaround range (focus on 2025 for final counting)
    stage1_map: Dict[str, List[pd.Timestamp]] = {}
    for code, g in daily.groupby('code', sort=False):
        listing_date = g['date'].iloc[0]
        use_monthly = listing_date < SPLIT_DATE
        rule = 'ME' if use_monthly else 'W-FRI'
        ma_window = PRE_WINDOW if use_monthly else POST_WINDOW
        frame = resample_ohlc(g[['date', 'open', 'high', 'low', 'close']], rule=rule)
        s1_dates = [d for d in stage1_signal_dates(frame, ma_window) if LOOKAROUND_START <= d <= LOOKAROUND_END]
        if s1_dates:
            stage1_map[code] = s1_dates

    candidate_codes = sorted(
        [
            code
            for code, dates in stage1_map.items()
            if any((d >= START_2025) and (d <= END_2025) for d in dates)
        ]
    )
    print('CANDIDATE_CODES_FROM_STAGE1_2025', len(candidate_codes))

    matched_rows = []
    stock_hits = 0
    total_signals = 0

    for idx, code in enumerate(candidate_codes, start=1):
        s1_dates = sorted([d for d in stage1_map.get(code, []) if START_2025 <= d <= END_2025])
        if not s1_dates:
            continue

        m15_df = load_15m_stock(m15_file, code)
        if m15_df.empty:
            continue

        s2_dates = stage2_signal_dates_from_15m(m15_df)
        if not s2_dates:
            continue

        matched_a = []
        for a in s1_dates:
            if any(within_two_months(a, b) for b in s2_dates):
                matched_a.append(a)

        if matched_a:
            stock_hits += 1
            total_signals += len(matched_a)
            matched_rows.append(
                {
                    'stock_code': code,
                    'stage1_dates_2025': ';'.join([d.strftime('%Y-%m-%d') for d in s1_dates]),
                    'stage2_dates': ';'.join([d.strftime('%Y-%m-%d') for d in s2_dates]),
                    'matched_stage1_dates_2025': ';'.join([d.strftime('%Y-%m-%d') for d in matched_a]),
                    'matched_signal_count': len(matched_a),
                }
            )

        if idx % 100 == 0 or idx == len(candidate_codes):
            print(f'PROGRESS {idx}/{len(candidate_codes)} stock_hits={stock_hits} signal_count={total_signals}')

    print('RESULT_STOCK_COUNT', stock_hits)
    print('RESULT_SIGNAL_COUNT', total_signals)

    if matched_rows:
        out = pd.DataFrame(matched_rows)
        print('RESULT_SAMPLE_TOP5')
        print(out[['stock_code', 'matched_signal_count']].head(5).to_string(index=False))


if __name__ == '__main__':
    main()
