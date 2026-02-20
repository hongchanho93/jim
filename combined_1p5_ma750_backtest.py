import argparse
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from ma_new_low_ratio_scan import Config, collect_part1_signal_dates_for_code


DEFAULT_DATA_15M = "D:/codex/股票数据维护/数据/全市场15分钟数据.parquet"
DEFAULT_DAILY_DATA = "D:/codex/股票数据维护/数据/全市场历史数据.parquet"
DEFAULT_OUTPUT = "outputs/combined_1p5_ma750_signals.csv"

MA_WINDOW = 750
FORWARD_WINDOWS = (20, 40, 60)
RETURN_THRESHOLDS = (0.20, 0.35, 0.45)

TIME_TO_30_END: Dict[str, str] = {
    "09:45:00": "10:00:00",
    "10:00:00": "10:00:00",
    "10:15:00": "10:30:00",
    "10:30:00": "10:30:00",
    "10:45:00": "11:00:00",
    "11:00:00": "11:00:00",
    "11:15:00": "11:30:00",
    "11:30:00": "11:30:00",
    "13:15:00": "13:30:00",
    "13:30:00": "13:30:00",
    "13:45:00": "14:00:00",
    "14:00:00": "14:00:00",
    "14:15:00": "14:30:00",
    "14:30:00": "14:30:00",
    "14:45:00": "15:00:00",
    "15:00:00": "15:00:00",
}

TIME_TO_60_END: Dict[str, str] = {
    "09:45:00": "10:30:00",
    "10:00:00": "10:30:00",
    "10:15:00": "10:30:00",
    "10:30:00": "10:30:00",
    "10:45:00": "11:30:00",
    "11:00:00": "11:30:00",
    "11:15:00": "11:30:00",
    "11:30:00": "11:30:00",
    "13:15:00": "14:00:00",
    "13:30:00": "14:00:00",
    "13:45:00": "14:00:00",
    "14:00:00": "14:00:00",
    "14:15:00": "15:00:00",
    "14:30:00": "15:00:00",
    "14:45:00": "15:00:00",
    "15:00:00": "15:00:00",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Backtest combined strategy: Part1(1.5x low-point) + "
            "Part2(MA750 resonance then bullish alignment)."
        )
    )
    parser.add_argument("--data15", default=DEFAULT_DATA_15M, help="15m parquet path")
    parser.add_argument("--daily-data", default=DEFAULT_DAILY_DATA, help="Daily parquet path")
    parser.add_argument("--start-date", required=True, help="Signal start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", default="", help="Signal end date (YYYY-MM-DD), default latest")
    parser.add_argument("--out", default=DEFAULT_OUTPUT, help="Signal output CSV path")

    parser.add_argument("--tolerance", type=float, default=0.03, help="MA750 resonance tolerance")
    parser.add_argument(
        "--month-window",
        type=int,
        default=1,
        help="Calendar-month window for all month-based matching",
    )
    parser.add_argument(
        "--resonance-bull-month-window",
        type=int,
        default=0,
        help="Calendar-month window from resonance to bullish alignment; 0 means use --month-window",
    )
    parser.add_argument(
        "--part1-match-month-window",
        type=int,
        default=0,
        help="Calendar-month window for Part1 date matching around resonance; 0 means use --month-window",
    )
    parser.add_argument(
        "--warmup-days",
        type=int,
        default=400,
        help="15m warmup history days before start-date",
    )
    parser.add_argument(
        "--part1-history-days",
        type=int,
        default=4000,
        help="Daily history days before start-date for Part1 replay",
    )
    parser.add_argument("--progress-every", type=int, default=200, help="Progress print interval")
    parser.add_argument(
        "--workers",
        type=int,
        default=0,
        help="Parallel workers; <=0 means auto",
    )

    parser.add_argument("--split-date", default="2019-01-01")
    parser.add_argument("--pre-window", type=int, default=32)
    parser.add_argument("--post-window", type=int, default=64)
    parser.add_argument("--fractal-n", type=int, default=3)
    parser.add_argument("--min-multiple", type=float, default=2.5)
    parser.add_argument("--ratio-low", type=float, default=1.45)
    parser.add_argument("--ratio-high", type=float, default=1.55)
    parser.add_argument("--target-ratio", type=float, default=1.50)
    parser.add_argument("--weekly-rule", default="W-FRI")
    parser.add_argument("--monthly-rule", default="ME")
    return parser.parse_args()


def normalize_stock_code(value: object) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    if text.isdigit() and len(text) < 6:
        return text.zfill(6)
    return text


def _decode_stat_value(value: object) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, bytes):
        return value.decode("utf-8")
    if isinstance(value, str):
        return value
    return str(value)


def find_latest_trade_date(data_path: str) -> str:
    parquet = pq.ParquetFile(data_path)
    date_index = parquet.schema.names.index("date")
    max_dates: List[str] = []
    for rg_index in range(parquet.metadata.num_row_groups):
        stats = parquet.metadata.row_group(rg_index).column(date_index).statistics
        if stats is None:
            continue
        date_max = _decode_stat_value(stats.max)
        if date_max:
            max_dates.append(date_max)

    if max_dates:
        return max(max_dates)

    latest: Optional[str] = None
    for batch in parquet.iter_batches(columns=["date"], batch_size=500_000):
        for value in batch.column("date").to_pylist():
            if value is None:
                continue
            if latest is None or value > latest:
                latest = value
    if latest is None:
        raise RuntimeError(f"Cannot determine latest trade date from {data_path}")
    return latest


def format_date(base_date: str, days_back: int) -> str:
    return (datetime.strptime(base_date, "%Y-%m-%d") - timedelta(days=days_back)).strftime(
        "%Y-%m-%d"
    )


def as_timestamp(date_str: str, end_of_day: bool = False) -> pd.Timestamp:
    suffix = "23:59:59" if end_of_day else "00:00:00"
    return pd.Timestamp(f"{date_str} {suffix}")


def get_stock_universe(data_path: str, start_date: str, end_date: str) -> List[str]:
    table = pq.read_table(
        data_path,
        columns=["stock_code"],
        filters=[("date", ">=", start_date), ("date", "<=", end_date)],
    )
    codes = table.column("stock_code").to_pylist()
    normalized = {normalize_stock_code(code) for code in codes}
    return sorted({code for code in normalized if code})


def load_stock_data_15m(
    data_path: str,
    stock_code: str,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    table = pq.read_table(
        data_path,
        columns=["stock_code", "stock_name", "date", "time", "open", "high", "low", "close"],
        filters=[
            ("stock_code", "=", stock_code),
            ("date", ">=", start_date),
            ("date", "<=", end_date),
        ],
    )
    if table.num_rows == 0:
        return pd.DataFrame()
    return table.to_pandas()


def load_stock_data_daily(
    data_path: str,
    stock_code: str,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    table = pq.read_table(
        data_path,
        columns=["stock_code", "stock_name", "date", "open", "high", "low", "close", "turnover"],
        filters=[
            ("stock_code", "=", stock_code),
            ("date", ">=", start_date),
            ("date", "<=", end_date),
        ],
    )
    if table.num_rows == 0:
        return pd.DataFrame()
    return table.to_pandas()


def add_clean_close(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    critical_cols = ["open", "high", "low", "close"]
    invalid = out[critical_cols].isna().any(axis=1)
    invalid |= (out[critical_cols] <= 0).any(axis=1)
    invalid |= out["date"].isna() | out["time"].isna()
    out["close_clean"] = out["close"].where(~invalid, np.nan)
    return out


def rolling_ma(series: pd.Series, window: int) -> pd.Series:
    return series.rolling(window=window, min_periods=1).mean()


def build_bucket_close(
    df_15m: pd.DataFrame,
    time_map: Dict[str, str],
    close_col_name: str,
) -> pd.DataFrame:
    valid = df_15m[df_15m["close_clean"].notna()].copy()
    if valid.empty:
        return pd.DataFrame(columns=["date", "ts", close_col_name])

    valid["bucket_end_time"] = valid["time"].map(time_map)
    valid = valid[valid["bucket_end_time"].notna()].copy()
    if valid.empty:
        return pd.DataFrame(columns=["date", "ts", close_col_name])

    valid = valid.sort_values("ts")
    grouped = (
        valid.groupby(["date", "bucket_end_time"], as_index=False)["close_clean"]
        .last()
        .rename(columns={"close_clean": close_col_name})
    )
    grouped["ts"] = pd.to_datetime(
        grouped["date"] + " " + grouped["bucket_end_time"],
        errors="coerce",
    )
    grouped = grouped.dropna(subset=["ts"]).sort_values("ts").reset_index(drop=True)
    return grouped[["date", "ts", close_col_name]]


def build_intraday_daily_states(
    stock_df_15m: pd.DataFrame,
    start_date: str,
    end_date: str,
    tolerance: float,
) -> pd.DataFrame:
    empty_cols = ["date", "date_dt", "resonance", "bull"]
    if stock_df_15m.empty:
        return pd.DataFrame(columns=empty_cols)

    df = stock_df_15m.copy()
    df["date"] = df["date"].astype(str).str[:10]
    df["time"] = df["time"].astype(str).str[:8]
    df["ts"] = pd.to_datetime(df["date"] + " " + df["time"], errors="coerce")
    df = df.dropna(subset=["ts"]).sort_values("ts").reset_index(drop=True)
    if df.empty:
        return pd.DataFrame(columns=empty_cols)

    df = add_clean_close(df)
    df["ma15"] = rolling_ma(df["close_clean"], MA_WINDOW)

    df15 = df[["date", "ts", "ma15"]].copy()
    df30 = build_bucket_close(df, TIME_TO_30_END, "close30")
    df60 = build_bucket_close(df, TIME_TO_60_END, "close60")
    if df30.empty or df60.empty:
        return pd.DataFrame(columns=empty_cols)

    df30["ma30"] = rolling_ma(df30["close30"], MA_WINDOW)
    df60["ma60"] = rolling_ma(df60["close60"], MA_WINDOW)

    mask_window = (df60["ts"] >= as_timestamp(start_date)) & (
        df60["ts"] <= as_timestamp(end_date, end_of_day=True)
    )
    base = df60.loc[mask_window, ["date", "ts", "ma60"]].copy()
    if base.empty:
        return pd.DataFrame(columns=empty_cols)

    right15 = df15[["date", "ts", "ma15"]].sort_values("ts")
    right30 = df30[["date", "ts", "ma30"]].sort_values("ts")
    base = base.sort_values("ts")

    merged = pd.merge_asof(base, right15, on="ts", by="date", direction="backward")
    merged = pd.merge_asof(merged, right30, on="ts", by="date", direction="backward")
    merged = merged.dropna(subset=["ma15", "ma30", "ma60"]).copy()
    if merged.empty:
        return pd.DataFrame(columns=empty_cols)

    ma_min = merged[["ma15", "ma30", "ma60"]].min(axis=1)
    ma_max = merged[["ma15", "ma30", "ma60"]].max(axis=1)
    deviation = (ma_max - ma_min) / ma_min
    merged["resonance"] = deviation <= tolerance
    merged["bull"] = (merged["ma15"] >= merged["ma30"]) & (merged["ma30"] >= merged["ma60"])

    day_state = (
        merged.groupby("date", as_index=False)[["resonance", "bull"]]
        .max()
        .sort_values("date")
        .reset_index(drop=True)
    )
    day_state["date_dt"] = pd.to_datetime(day_state["date"], errors="coerce")
    day_state = day_state.dropna(subset=["date_dt"]).sort_values("date_dt").reset_index(drop=True)
    return day_state[empty_cols]


def build_bull_phases(day_state: pd.DataFrame) -> List[Tuple[pd.Timestamp, pd.Timestamp]]:
    if day_state.empty:
        return []
    states = day_state.sort_values("date_dt").reset_index(drop=True)
    bull_values = states["bull"].astype(bool).tolist()
    dates = states["date_dt"].tolist()
    phases: List[Tuple[pd.Timestamp, pd.Timestamp]] = []
    for i, is_bull in enumerate(bull_values):
        if not is_bull:
            continue
        prev_bull = bull_values[i - 1] if i > 0 else False
        if not prev_bull:
            start = dates[i]
            j = i
            while j + 1 < len(bull_values) and bull_values[j + 1]:
                j += 1
            end = dates[j]
            phases.append((start, end))
    return phases


def within_forward_calendar_month(
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    month_window: int,
) -> bool:
    if end_date < start_date:
        return False
    return bool(end_date <= start_date + pd.DateOffset(months=month_window))


def build_part2_candidates(
    day_state: pd.DataFrame,
    signal_start: pd.Timestamp,
    signal_end: pd.Timestamp,
    month_window: int,
) -> Tuple[List[Dict[str, pd.Timestamp]], Optional[str]]:
    if day_state.empty:
        return [], "no_intraday_state"

    resonance_dates = sorted(day_state.loc[day_state["resonance"], "date_dt"].tolist())
    if not resonance_dates:
        return [], "no_resonance"

    bull_phases = build_bull_phases(day_state)
    if not bull_phases:
        return [], "no_bull_phase"

    used_resonance_dates: set[pd.Timestamp] = set()
    events: List[Dict[str, pd.Timestamp]] = []
    previous_phase_end: Optional[pd.Timestamp] = None

    for bull_start, bull_end in bull_phases:
        lower = bull_start - pd.DateOffset(months=month_window)
        if previous_phase_end is not None:
            lower = max(lower, previous_phase_end + pd.Timedelta(days=1))

        candidates = [
            r
            for r in resonance_dates
            if lower <= r <= bull_start and r not in used_resonance_dates
        ]
        selected: Optional[pd.Timestamp] = None
        for resonance_date in candidates:
            if within_forward_calendar_month(resonance_date, bull_start, month_window):
                selected = resonance_date
                break

        if selected is not None:
            used_resonance_dates.add(selected)
            if signal_start <= selected <= signal_end:
                events.append(
                    {
                        "resonance_date": selected,
                        "bull_start_date": bull_start,
                        "bull_end_date": bull_end,
                    }
                )
        previous_phase_end = bull_end

    if not events:
        return [], "no_part2_candidate"
    return events, None


def build_month_turnover_map(daily_df: pd.DataFrame) -> Dict[int, float]:
    if daily_df.empty:
        return {}
    month_ord = daily_df["date_dt"].dt.year * 12 + daily_df["date_dt"].dt.month
    return daily_df.groupby(month_ord, sort=False)["turnover"].sum().to_dict()


def month_turnover_condition_ok(
    month_turnover_map: Dict[int, float],
    resonance_date: pd.Timestamp,
    lookback_months: int = 18,
    threshold: float = 1.0,
) -> bool:
    signal_ord = resonance_date.year * 12 + resonance_date.month
    begin = signal_ord - lookback_months
    for month_ord in range(begin, signal_ord):
        if month_turnover_map.get(month_ord, 0.0) > threshold:
            return True
    return False


def find_part1_match_date(
    part1_dates: Sequence[pd.Timestamp],
    resonance_date: pd.Timestamp,
    month_window: int,
) -> Optional[Tuple[pd.Timestamp, int]]:
    if not part1_dates:
        return None
    lower = resonance_date - pd.DateOffset(months=month_window)
    upper = resonance_date + pd.DateOffset(months=month_window)
    candidates = [d for d in part1_dates if lower <= d <= upper]
    if not candidates:
        return None
    selected = min(candidates, key=lambda d: (abs((d - resonance_date).days), d))
    gap_days = abs((selected - resonance_date).days)
    return selected, int(gap_days)


def prepare_daily_price_frame(stock_df_daily: pd.DataFrame) -> pd.DataFrame:
    if stock_df_daily.empty:
        return pd.DataFrame(columns=["date_dt", "high", "close"])
    out = stock_df_daily.copy()
    out = out.dropna(subset=["date", "high", "close"])
    out["date_dt"] = pd.to_datetime(out["date"], errors="coerce")
    out = out.dropna(subset=["date_dt"])
    out["high"] = pd.to_numeric(out["high"], errors="coerce")
    out["close"] = pd.to_numeric(out["close"], errors="coerce")
    out = out[(out["high"] > 0) & (out["close"] > 0)]
    out = out.sort_values("date_dt").drop_duplicates(subset=["date_dt"], keep="last")
    return out[["date_dt", "high", "close"]].reset_index(drop=True)


def evaluate_forward_returns(
    daily_price: pd.DataFrame,
    bull_start_date: pd.Timestamp,
    windows: Iterable[int],
) -> Tuple[float, Dict[int, float], Dict[int, bool]]:
    returns: Dict[int, float] = {}
    valid: Dict[int, bool] = {}
    if daily_price.empty:
        for window in windows:
            returns[window] = np.nan
            valid[window] = False
        return float("nan"), returns, valid

    date_to_pos = {d: i for i, d in enumerate(daily_price["date_dt"])}
    pos = date_to_pos.get(bull_start_date)
    if pos is None:
        for window in windows:
            returns[window] = np.nan
            valid[window] = False
        return float("nan"), returns, valid

    base_close = float(daily_price.iloc[pos]["close"])
    if base_close <= 0:
        for window in windows:
            returns[window] = np.nan
            valid[window] = False
        return float("nan"), returns, valid

    for window in windows:
        future = daily_price.iloc[pos + 1 : pos + 1 + window]
        if len(future) < window:
            returns[window] = np.nan
            valid[window] = False
            continue
        max_high = float(future["high"].max())
        returns[window] = max_high / base_close - 1.0
        valid[window] = True
    return base_close, returns, valid


def build_summary_row(
    signal_df: pd.DataFrame,
    skipped_df: pd.DataFrame,
    stocks_total_scanned: int,
) -> pd.DataFrame:
    summary: Dict[str, object] = {
        "stocks_total_scanned": int(stocks_total_scanned),
        "signal_count": int(len(signal_df)),
        "signal_stocks": int(signal_df["stock_code"].nunique()) if not signal_df.empty else 0,
        "skipped_stocks": int(skipped_df["stock_code"].nunique()) if not skipped_df.empty else 0,
    }

    for window in FORWARD_WINDOWS:
        ret_col = f"ret_max_high_{window}d"
        valid_col = f"valid_{window}d"
        for threshold in RETURN_THRESHOLDS:
            pct = int(round(threshold * 100))
            prefix = f"hit_{window}d_{pct}"
            if signal_df.empty:
                num = 0
                den = 0
                ratio = np.nan
            else:
                den = int(signal_df[valid_col].fillna(False).sum())
                if den == 0:
                    num = 0
                    ratio = np.nan
                else:
                    num = int(
                        (
                            signal_df[valid_col].fillna(False)
                            & (signal_df[ret_col].fillna(-np.inf) >= threshold)
                        ).sum()
                    )
                    ratio = num / den
            summary[f"{prefix}_num"] = num
            summary[f"{prefix}_den"] = den
            summary[f"{prefix}_ratio"] = ratio

    return pd.DataFrame([summary])


def write_outputs(
    out_path: Path,
    signal_df: pd.DataFrame,
    summary_df: pd.DataFrame,
    skipped_df: pd.DataFrame,
) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    signal_df.to_csv(out_path, index=False)

    summary_out = out_path.with_name(f"{out_path.stem}_summary.csv")
    summary_df.to_csv(summary_out, index=False)

    skipped_summary = (
        skipped_df.groupby("reason", dropna=False).size().reset_index(name="count")
        if not skipped_df.empty
        else pd.DataFrame(columns=["reason", "count"])
    )
    skipped_out = out_path.with_name(f"{out_path.stem}_skipped_summary.csv")
    skipped_summary.to_csv(skipped_out, index=False)

    print(f"Saved signal rows: {out_path}")
    print(f"Saved summary: {summary_out}")
    print(f"Saved skipped summary: {skipped_out}")
    print(summary_df.to_string(index=False))


def process_one_stock(
    stock_code: str,
    args: argparse.Namespace,
    part1_cfg: Config,
    daily_start: str,
    latest_daily: str,
    warmup_start: str,
    analysis_end: str,
    signal_start_ts: pd.Timestamp,
    signal_end_ts: pd.Timestamp,
    resonance_bull_month_window: int,
    part1_match_month_window: int,
) -> Tuple[List[Dict[str, object]], Optional[str]]:
    try:
        daily_df = load_stock_data_daily(args.daily_data, stock_code, daily_start, latest_daily)
        if daily_df.empty:
            return [], "no_daily_data"

        daily_df = daily_df.copy()
        daily_df["stock_code"] = daily_df["stock_code"].map(normalize_stock_code)
        daily_df["date_dt"] = pd.to_datetime(daily_df["date"], errors="coerce")
        daily_df = daily_df.dropna(subset=["date_dt"]).sort_values("date_dt").reset_index(drop=True)
        if daily_df.empty:
            return [], "no_valid_daily_data"
        daily_df["turnover"] = pd.to_numeric(daily_df["turnover"], errors="coerce").fillna(0.0)
        month_turnover_map = build_month_turnover_map(daily_df)

        part1_input = daily_df.rename(columns={"stock_code": "code"})[
            ["date", "code", "open", "high", "low", "close"]
        ].copy()
        part1_input["date"] = pd.to_datetime(part1_input["date"], errors="coerce")
        part1_input = part1_input.dropna(subset=["date"])
        if part1_input.empty:
            return [], "no_part1_input_data"
        listing_date = pd.Timestamp(daily_df["date_dt"].iloc[0])
        part1_dates_raw, part1_reason = collect_part1_signal_dates_for_code(
            code=stock_code,
            df_code_daily=part1_input,
            listing_date=listing_date,
            cfg=part1_cfg,
        )
        if not part1_dates_raw:
            return [], part1_reason or "no_part1_signal"
        part1_dates = sorted({pd.Timestamp(d) for d in part1_dates_raw})

        intraday_df = load_stock_data_15m(args.data15, stock_code, warmup_start, analysis_end)
        if intraday_df.empty:
            return [], "no_intraday_data"

        day_state = build_intraday_daily_states(
            intraday_df,
            start_date=args.start_date,
            end_date=analysis_end,
            tolerance=args.tolerance,
        )
        part2_events, part2_reason = build_part2_candidates(
            day_state=day_state,
            signal_start=signal_start_ts,
            signal_end=signal_end_ts,
            month_window=resonance_bull_month_window,
        )
        if not part2_events:
            return [], part2_reason or "no_part2_candidate"

        daily_price = prepare_daily_price_frame(daily_df)
        stock_names = intraday_df["stock_name"].dropna()
        stock_name = str(stock_names.iloc[0]) if not stock_names.empty else ""

        rows: List[Dict[str, object]] = []
        for event in part2_events:
            resonance_date = event["resonance_date"]
            bull_start_date = event["bull_start_date"]
            bull_end_date = event["bull_end_date"]
            if not month_turnover_condition_ok(
                month_turnover_map=month_turnover_map,
                resonance_date=resonance_date,
                lookback_months=18,
                threshold=1.0,
            ):
                continue

            matched = find_part1_match_date(
                part1_dates,
                resonance_date,
                part1_match_month_window,
            )
            if matched is None:
                continue
            part1_date, gap_days = matched
            base_close, ret_map, valid_map = evaluate_forward_returns(
                daily_price=daily_price,
                bull_start_date=bull_start_date,
                windows=FORWARD_WINDOWS,
            )

            row: Dict[str, object] = {
                "stock_code": normalize_stock_code(stock_code),
                "stock_name": stock_name,
                "signal_date": resonance_date.date().isoformat(),
                "resonance_date": resonance_date.date().isoformat(),
                "bull_start_date": bull_start_date.date().isoformat(),
                "bull_end_date": bull_end_date.date().isoformat(),
                "part1_date": part1_date.date().isoformat(),
                "part1_res_gap_days": int(gap_days),
                "resonance_to_bull_days": int((bull_start_date - resonance_date).days),
                "base_close_bull_start": base_close,
            }
            for window in FORWARD_WINDOWS:
                row[f"ret_max_high_{window}d"] = ret_map[window]
                row[f"valid_{window}d"] = bool(valid_map[window])
            rows.append(row)

        if not rows:
            return [], "no_part1_or_turnover_match"
        return rows, None
    except Exception as exc:
        return [], f"error:{type(exc).__name__}"


def run_backtest(args: argparse.Namespace) -> None:
    latest_15m = find_latest_trade_date(args.data15)
    latest_daily = find_latest_trade_date(args.daily_data)
    end_date = args.end_date or latest_15m
    if end_date > latest_15m:
        raise SystemExit(f"--end-date {end_date} exceeds latest 15m date {latest_15m}")
    if args.start_date > end_date:
        raise SystemExit("--start-date cannot be later than --end-date")
    if args.month_window < 1:
        raise SystemExit("--month-window must be >= 1")
    if args.resonance_bull_month_window < 0:
        raise SystemExit("--resonance-bull-month-window must be >= 0")
    if args.part1_match_month_window < 0:
        raise SystemExit("--part1-match-month-window must be >= 0")
    if args.tolerance <= 0:
        raise SystemExit("--tolerance must be > 0")

    resonance_bull_month_window = (
        int(args.resonance_bull_month_window)
        if int(args.resonance_bull_month_window) > 0
        else int(args.month_window)
    )
    part1_match_month_window = (
        int(args.part1_match_month_window)
        if int(args.part1_match_month_window) > 0
        else int(args.month_window)
    )

    signal_start_ts = pd.Timestamp(args.start_date)
    signal_end_ts = pd.Timestamp(end_date)
    analysis_end_ts = min(
        pd.Timestamp(latest_15m),
        signal_end_ts + pd.DateOffset(months=resonance_bull_month_window),
    )
    analysis_end = analysis_end_ts.date().isoformat()
    warmup_start = format_date(args.start_date, args.warmup_days)
    daily_start = format_date(args.start_date, args.part1_history_days)

    print(f"[INFO] start_date={args.start_date}")
    print(f"[INFO] end_date={end_date}")
    print(f"[INFO] analysis_end_for_part2={analysis_end}")
    print(f"[INFO] warmup_start_15m={warmup_start}")
    print(f"[INFO] daily_start_for_part1={daily_start}")
    print(f"[INFO] tolerance={args.tolerance}")
    print(f"[INFO] resonance_bull_month_window={resonance_bull_month_window}")
    print(f"[INFO] part1_match_month_window={part1_match_month_window}")

    stock_codes = get_stock_universe(args.data15, args.start_date, end_date)
    print(f"[INFO] stock_universe={len(stock_codes)}")
    workers = int(args.workers)
    if workers <= 0:
        workers = max(1, min(8, (os.cpu_count() or 1)))
    print(f"[INFO] workers={workers}")

    part1_cfg = Config(
        data="",
        out="",
        split_date=pd.Timestamp(args.split_date),
        pre_window=int(args.pre_window),
        post_window=int(args.post_window),
        fractal_n=int(args.fractal_n),
        min_multiple=float(args.min_multiple),
        ratio_low=float(args.ratio_low),
        ratio_high=float(args.ratio_high),
        target_ratio=float(args.target_ratio),
        weekly_rule=str(args.weekly_rule),
        monthly_rule=str(args.monthly_rule),
    )

    results: List[Dict[str, object]] = []
    skipped: List[Dict[str, str]] = []

    if workers <= 1:
        for idx, stock_code in enumerate(stock_codes, start=1):
            rows, reason = process_one_stock(
                stock_code=stock_code,
                args=args,
                part1_cfg=part1_cfg,
                daily_start=daily_start,
                latest_daily=latest_daily,
                warmup_start=warmup_start,
                analysis_end=analysis_end,
                signal_start_ts=signal_start_ts,
                signal_end_ts=signal_end_ts,
                resonance_bull_month_window=resonance_bull_month_window,
                part1_match_month_window=part1_match_month_window,
            )
            if rows:
                results.extend(rows)
            else:
                skipped.append({"stock_code": stock_code, "reason": reason or "unknown"})

            if idx % args.progress_every == 0 or idx == len(stock_codes):
                print(
                    f"[PROGRESS] {idx}/{len(stock_codes)} stocks processed, "
                    f"signals={len(results)}, skipped={len(skipped)}"
                )
    else:
        futures = {}
        with ThreadPoolExecutor(max_workers=workers) as executor:
            for stock_code in stock_codes:
                fut = executor.submit(
                    process_one_stock,
                    stock_code,
                    args,
                    part1_cfg,
                    daily_start,
                    latest_daily,
                    warmup_start,
                    analysis_end,
                    signal_start_ts,
                    signal_end_ts,
                    resonance_bull_month_window,
                    part1_match_month_window,
                )
                futures[fut] = stock_code

            done_count = 0
            for fut in as_completed(futures):
                done_count += 1
                stock_code = futures[fut]
                rows, reason = fut.result()
                if rows:
                    results.extend(rows)
                else:
                    skipped.append({"stock_code": stock_code, "reason": reason or "unknown"})

                if done_count % args.progress_every == 0 or done_count == len(stock_codes):
                    print(
                        f"[PROGRESS] {done_count}/{len(stock_codes)} stocks processed, "
                        f"signals={len(results)}, skipped={len(skipped)}"
                    )

    signal_df = pd.DataFrame(results)
    if not signal_df.empty:
        signal_df = signal_df.sort_values(["signal_date", "stock_code"]).reset_index(drop=True)

    skipped_df = pd.DataFrame(skipped)
    summary_df = build_summary_row(
        signal_df=signal_df,
        skipped_df=skipped_df,
        stocks_total_scanned=len(stock_codes),
    )

    write_outputs(Path(args.out), signal_df, summary_df, skipped_df)


def main() -> None:
    args = parse_args()
    run_backtest(args)


if __name__ == "__main__":
    main()
