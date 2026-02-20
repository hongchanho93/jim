import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class Config:
    data: str
    out: str
    split_date: pd.Timestamp
    pre_window: int
    post_window: int
    fractal_n: int
    min_multiple: float
    ratio_low: float
    ratio_high: float
    target_ratio: float
    weekly_rule: str
    monthly_rule: str


@dataclass(frozen=True)
class RallyEvent:
    bottom_idx: int
    peak_idx: int
    bottom_price: float
    peak_price: float
    multiple: float
    bottom_confirm_idx: int
    peak_confirm_idx: int


@dataclass(frozen=True)
class NewLowPoint:
    idx: int
    price: float


@dataclass(frozen=True)
class CausalEngineResult:
    event: Optional[RallyEvent]
    new_low: Optional[NewLowPoint]
    last_signal_idx: Optional[int]
    signal_count: int
    signal_indices: Tuple[int, ...]


def parse_args() -> Config:
    parser = argparse.ArgumentParser(
        description=(
            "Scan stocks with split logic: listed before 2019 uses monthly MA32, "
            "otherwise weekly MA64, then compare MA/new-low ratio."
        )
    )
    parser.add_argument("--data", default="all_data_baostock_2025.parquet")
    parser.add_argument("--out", default="outputs/ma_new_low_ratio_signals.csv")
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
    args = parser.parse_args()

    split_date = pd.Timestamp(args.split_date)
    if args.fractal_n < 1:
        raise SystemExit("--fractal-n must be >= 1")
    if args.pre_window < 2 or args.post_window < 2:
        raise SystemExit("MA window must be >= 2")
    if args.min_multiple <= 0:
        raise SystemExit("--min-multiple must be > 0")
    if args.ratio_low <= 0 or args.ratio_high <= 0:
        raise SystemExit("ratio bounds must be > 0")
    if args.ratio_low > args.ratio_high:
        raise SystemExit("--ratio-low cannot be greater than --ratio-high")
    if args.target_ratio <= 0:
        raise SystemExit("--target-ratio must be > 0")

    return Config(
        data=args.data,
        out=args.out,
        split_date=split_date,
        pre_window=args.pre_window,
        post_window=args.post_window,
        fractal_n=args.fractal_n,
        min_multiple=args.min_multiple,
        ratio_low=args.ratio_low,
        ratio_high=args.ratio_high,
        target_ratio=args.target_ratio,
        weekly_rule=args.weekly_rule,
        monthly_rule=args.monthly_rule,
    )


def _find_first_existing(df: pd.DataFrame, names: List[str]) -> Optional[str]:
    for name in names:
        if name in df.columns:
            return name
    return None


def normalize_input_columns(df: pd.DataFrame) -> pd.DataFrame:
    date_col = _find_first_existing(df, ["date", "trade_date", "datetime", "dt"])
    code_col = _find_first_existing(df, ["code", "symbol", "ts_code", "ticker"])
    open_col = _find_first_existing(df, ["open"])
    high_col = _find_first_existing(df, ["high"])
    low_col = _find_first_existing(df, ["low"])
    close_col = _find_first_existing(df, ["close"])
    list_date_col = _find_first_existing(df, ["list_date", "ipo_date", "listing_date"])

    required_missing = [
        name
        for name, col in [
            ("date", date_col),
            ("code", code_col),
            ("high", high_col),
            ("low", low_col),
            ("close", close_col),
        ]
        if col is None
    ]
    if required_missing:
        raise SystemExit(f"Missing required fields: {required_missing}")

    out = pd.DataFrame(
        {
            "date": pd.to_datetime(df[date_col], errors="coerce"),
            "code": df[code_col].astype(str),
            "high": pd.to_numeric(df[high_col], errors="coerce"),
            "low": pd.to_numeric(df[low_col], errors="coerce"),
            "close": pd.to_numeric(df[close_col], errors="coerce"),
        }
    )
    if open_col is None:
        out["open"] = out["close"]
    else:
        out["open"] = pd.to_numeric(df[open_col], errors="coerce")
    if list_date_col is not None:
        out["list_date"] = pd.to_datetime(df[list_date_col], errors="coerce")

    out = out.dropna(subset=["date", "code", "open", "high", "low", "close"])
    out = out.sort_values(["code", "date"]).reset_index(drop=True)
    return out


def derive_listing_dates(df: pd.DataFrame) -> pd.Series:
    first_trade = df.groupby("code", sort=False)["date"].min()
    if "list_date" not in df.columns:
        return first_trade

    listed = df.dropna(subset=["list_date"]).groupby("code", sort=False)["list_date"].min()
    listing_dates = first_trade.copy()
    listing_dates.loc[listed.index] = listed
    return listing_dates


def resample_ohlc(df_code: pd.DataFrame, rule: str) -> pd.DataFrame:
    g = df_code.set_index("date").sort_index()
    out = g.resample(rule, label="right", closed="right").agg(
        {"open": "first", "high": "max", "low": "min", "close": "last"}
    )
    out = out.dropna(subset=["open", "high", "low", "close"]).reset_index()
    return out


def calc_sma(series: pd.Series, window: int) -> pd.Series:
    return series.rolling(window=window, min_periods=window).mean()


def detect_fractal_swings(df: pd.DataFrame, n: int) -> Tuple[List[int], List[int]]:
    lows = df["low"].to_numpy(dtype=float)
    highs = df["high"].to_numpy(dtype=float)
    bottoms: List[int] = []
    peaks: List[int] = []

    if len(df) < 2 * n + 1:
        return bottoms, peaks

    for i in range(n, len(df) - n):
        left_low = lows[i - n : i]
        right_low = lows[i + 1 : i + n + 1]
        left_high = highs[i - n : i]
        right_high = highs[i + 1 : i + n + 1]

        if lows[i] < float(np.min(left_low)) and lows[i] < float(np.min(right_low)):
            bottoms.append(i)
        if highs[i] > float(np.max(left_high)) and highs[i] > float(np.max(right_high)):
            peaks.append(i)

    return bottoms, peaks


def find_latest_rally_event(
    df: pd.DataFrame, bottoms: List[int], peaks: List[int], min_multiple: float
) -> Optional[RallyEvent]:
    if not bottoms or not peaks:
        return None

    lows = df["low"].to_numpy(dtype=float)
    highs = df["high"].to_numpy(dtype=float)
    bottoms_sorted = sorted(bottoms)
    peaks_sorted = sorted(peaks)

    candidates: List[RallyEvent] = []
    for peak_idx in peaks_sorted:
        prior_bottoms = [b for b in bottoms_sorted if b < peak_idx]
        if not prior_bottoms:
            continue
        bottom_idx = prior_bottoms[-1]
        bottom_price = float(lows[bottom_idx])
        peak_price = float(highs[peak_idx])
        if bottom_price <= 0:
            continue
        multiple = peak_price / bottom_price
        if multiple >= min_multiple:
            candidates.append(
                RallyEvent(
                    bottom_idx=bottom_idx,
                    peak_idx=peak_idx,
                    bottom_price=bottom_price,
                    peak_price=peak_price,
                    multiple=multiple,
                    bottom_confirm_idx=bottom_idx,
                    peak_confirm_idx=peak_idx,
                )
            )

    if not candidates:
        return None
    return candidates[-1]


def find_new_low_since_peak(df: pd.DataFrame, peak_idx: int) -> Optional[NewLowPoint]:
    if peak_idx + 1 >= len(df):
        return None
    lows = df["low"].to_numpy(dtype=float)
    tail = lows[peak_idx + 1 :]
    min_pos = int(np.argmin(tail))
    idx = peak_idx + 1 + min_pos
    return NewLowPoint(idx=idx, price=float(lows[idx]))


def _is_fractal_bottom_at(lows: np.ndarray, idx: int, n: int) -> bool:
    left_low = lows[idx - n : idx]
    right_low = lows[idx + 1 : idx + n + 1]
    return bool(lows[idx] < float(np.min(left_low)) and lows[idx] < float(np.min(right_low)))


def _is_fractal_peak_at(highs: np.ndarray, idx: int, n: int) -> bool:
    left_high = highs[idx - n : idx]
    right_high = highs[idx + 1 : idx + n + 1]
    return bool(highs[idx] > float(np.max(left_high)) and highs[idx] > float(np.max(right_high)))


def run_causal_engine(frame: pd.DataFrame, cfg: Config) -> CausalEngineResult:
    """Single-pass causal engine: only uses data available up to current bar."""
    n = cfg.fractal_n
    lows = frame["low"].to_numpy(dtype=float)
    highs = frame["high"].to_numpy(dtype=float)
    ma = frame["ma"].to_numpy(dtype=float)

    if len(frame) < 2 * n + 1:
        return CausalEngineResult(
            event=None,
            new_low=None,
            last_signal_idx=None,
            signal_count=0,
            signal_indices=tuple(),
        )

    bottoms: List[int] = []
    bottom_confirm_idx: Dict[int, int] = {}
    active_event: Optional[RallyEvent] = None
    rolling_new_low: Optional[NewLowPoint] = None
    signal_indices: List[int] = []
    in_signal_zone = False
    zone_best_idx: Optional[int] = None
    zone_best_distance = float("inf")

    def close_zone_if_needed() -> None:
        nonlocal in_signal_zone, zone_best_idx, zone_best_distance
        if in_signal_zone and zone_best_idx is not None:
            signal_indices.append(zone_best_idx)
        in_signal_zone = False
        zone_best_idx = None
        zone_best_distance = float("inf")

    for t in range(len(frame)):
        candidate = t - n
        if candidate >= n:
            if _is_fractal_bottom_at(lows, candidate, n):
                bottoms.append(candidate)
                bottom_confirm_idx[candidate] = t

            if _is_fractal_peak_at(highs, candidate, n):
                prior_bottoms = [b for b in bottoms if b < candidate]
                if prior_bottoms:
                    b = prior_bottoms[-1]
                    bottom_price = float(lows[b])
                    peak_price = float(highs[candidate])
                    if bottom_price > 0:
                        multiple = peak_price / bottom_price
                        if multiple >= cfg.min_multiple:
                            active_event = RallyEvent(
                                bottom_idx=b,
                                peak_idx=candidate,
                                bottom_price=bottom_price,
                                peak_price=peak_price,
                                multiple=multiple,
                                bottom_confirm_idx=bottom_confirm_idx[b],
                                peak_confirm_idx=t,
                            )
                            rolling_new_low = None
                            signal_indices = []
                            in_signal_zone = False
                            zone_best_idx = None
                            zone_best_distance = float("inf")

        if active_event is None or t <= active_event.peak_idx:
            continue

        if rolling_new_low is None or lows[t] < rolling_new_low.price:
            rolling_new_low = NewLowPoint(idx=t, price=float(lows[t]))

        if rolling_new_low.price <= 0 or np.isnan(ma[t]):
            close_zone_if_needed()
            continue
        ratio_t = float(ma[t]) / rolling_new_low.price
        in_band = cfg.ratio_low <= ratio_t <= cfg.ratio_high
        if in_band:
            current_distance = abs(ratio_t - cfg.target_ratio)
            if not in_signal_zone:
                in_signal_zone = True
                zone_best_idx = t
                zone_best_distance = current_distance
            elif current_distance < zone_best_distance:
                zone_best_idx = t
                zone_best_distance = current_distance
        else:
            close_zone_if_needed()

    close_zone_if_needed()
    last_signal_idx = signal_indices[-1] if signal_indices else None

    return CausalEngineResult(
        event=active_event,
        new_low=rolling_new_low,
        last_signal_idx=last_signal_idx,
        signal_count=len(signal_indices),
        signal_indices=tuple(signal_indices),
    )


def collect_causal_signal_indices_all_events(frame: pd.DataFrame, cfg: Config) -> Tuple[int, ...]:
    """Collect causal signal indices across all qualifying rally events in one pass."""
    n = cfg.fractal_n
    lows = frame["low"].to_numpy(dtype=float)
    highs = frame["high"].to_numpy(dtype=float)
    ma = frame["ma"].to_numpy(dtype=float)

    if len(frame) < 2 * n + 1:
        return tuple()

    bottoms: List[int] = []
    bottom_confirm_idx: Dict[int, int] = {}
    active_event: Optional[RallyEvent] = None
    rolling_new_low: Optional[NewLowPoint] = None
    active_event_signals: List[int] = []
    all_signals: List[int] = []
    in_signal_zone = False
    zone_best_idx: Optional[int] = None
    zone_best_distance = float("inf")

    def close_zone_if_needed() -> None:
        nonlocal in_signal_zone, zone_best_idx, zone_best_distance
        if in_signal_zone and zone_best_idx is not None:
            active_event_signals.append(zone_best_idx)
        in_signal_zone = False
        zone_best_idx = None
        zone_best_distance = float("inf")

    def roll_event_if_needed() -> None:
        nonlocal active_event_signals
        close_zone_if_needed()
        if active_event_signals:
            all_signals.extend(active_event_signals)
        active_event_signals = []

    for t in range(len(frame)):
        candidate = t - n
        if candidate >= n:
            if _is_fractal_bottom_at(lows, candidate, n):
                bottoms.append(candidate)
                bottom_confirm_idx[candidate] = t

            if _is_fractal_peak_at(highs, candidate, n):
                prior_bottoms = [b for b in bottoms if b < candidate]
                if prior_bottoms:
                    b = prior_bottoms[-1]
                    bottom_price = float(lows[b])
                    peak_price = float(highs[candidate])
                    if bottom_price > 0:
                        multiple = peak_price / bottom_price
                        if multiple >= cfg.min_multiple:
                            roll_event_if_needed()
                            active_event = RallyEvent(
                                bottom_idx=b,
                                peak_idx=candidate,
                                bottom_price=bottom_price,
                                peak_price=peak_price,
                                multiple=multiple,
                                bottom_confirm_idx=bottom_confirm_idx[b],
                                peak_confirm_idx=t,
                            )
                            rolling_new_low = None

        if active_event is None or t <= active_event.peak_idx:
            continue

        if rolling_new_low is None or lows[t] < rolling_new_low.price:
            rolling_new_low = NewLowPoint(idx=t, price=float(lows[t]))

        if rolling_new_low.price <= 0 or np.isnan(ma[t]):
            close_zone_if_needed()
            continue
        ratio_t = float(ma[t]) / rolling_new_low.price
        in_band = cfg.ratio_low <= ratio_t <= cfg.ratio_high
        if in_band:
            current_distance = abs(ratio_t - cfg.target_ratio)
            if not in_signal_zone:
                in_signal_zone = True
                zone_best_idx = t
                zone_best_distance = current_distance
            elif current_distance < zone_best_distance:
                zone_best_idx = t
                zone_best_distance = current_distance
        else:
            close_zone_if_needed()

    roll_event_if_needed()
    return tuple(all_signals)


def collect_part1_signal_dates_for_code(
    code: str, df_code_daily: pd.DataFrame, listing_date: pd.Timestamp, cfg: Config
) -> Tuple[List[str], Optional[str]]:
    group_type = "monthly_ma32" if listing_date < cfg.split_date else "weekly_ma64"
    rule = cfg.monthly_rule if group_type == "monthly_ma32" else cfg.weekly_rule
    ma_window = cfg.pre_window if group_type == "monthly_ma32" else cfg.post_window

    frame = resample_ohlc(df_code_daily, rule=rule)
    min_required = max(ma_window, 2 * cfg.fractal_n + 1) + 1
    if len(frame) < min_required:
        return [], "insufficient_history"

    frame["ma"] = calc_sma(frame["close"], ma_window)
    if frame["ma"].notna().sum() == 0:
        return [], "insufficient_ma"

    signal_indices = collect_causal_signal_indices_all_events(frame, cfg)
    signal_dates = [
        pd.Timestamp(frame.loc[idx, "date"]).date().isoformat() for idx in signal_indices
    ]
    if not signal_dates:
        return [], "no_signal"
    return signal_dates, None


def scan_one_code(
    code: str, df_code_daily: pd.DataFrame, listing_date: pd.Timestamp, cfg: Config
) -> Tuple[Optional[Dict[str, object]], Optional[str]]:
    group_type = "monthly_ma32" if listing_date < cfg.split_date else "weekly_ma64"
    rule = cfg.monthly_rule if group_type == "monthly_ma32" else cfg.weekly_rule
    ma_window = cfg.pre_window if group_type == "monthly_ma32" else cfg.post_window

    frame = resample_ohlc(df_code_daily, rule=rule)
    min_required = max(ma_window, 2 * cfg.fractal_n + 1) + 1
    if len(frame) < min_required:
        return None, "insufficient_history"

    frame["ma"] = calc_sma(frame["close"], ma_window)
    ma_value = float(frame["ma"].iloc[-1])
    if np.isnan(ma_value):
        return None, "insufficient_ma"

    engine = run_causal_engine(frame, cfg)
    if engine.event is None:
        return None, "no_rally_event"
    event = engine.event

    if engine.new_low is None:
        return None, "no_post_peak_bars"
    new_low = engine.new_low

    if new_low.price <= 0:
        return None, "invalid_new_low"

    ratio = ma_value / new_low.price
    signal_dates = [
        pd.Timestamp(frame.loc[idx, "date"]).date().isoformat() for idx in engine.signal_indices
    ]
    is_signal = engine.signal_count > 0

    row: Dict[str, object] = {
        "code": code,
        "list_date": pd.Timestamp(listing_date).date().isoformat(),
        "group_type": group_type,
        "timeframe_rule": rule,
        "ma_window": int(ma_window),
        "fractal_n": int(cfg.fractal_n),
        "bottom_date": pd.Timestamp(frame.loc[event.bottom_idx, "date"]).date().isoformat(),
        "bottom_confirm_date": pd.Timestamp(frame.loc[event.bottom_confirm_idx, "date"]).date().isoformat(),
        "bottom_price": round(event.bottom_price, 6),
        "peak_date": pd.Timestamp(frame.loc[event.peak_idx, "date"]).date().isoformat(),
        "peak_confirm_date": pd.Timestamp(frame.loc[event.peak_confirm_idx, "date"]).date().isoformat(),
        "peak_price": round(event.peak_price, 6),
        "rally_multiple": round(event.multiple, 6),
        "new_low_date": pd.Timestamp(frame.loc[new_low.idx, "date"]).date().isoformat(),
        "new_low_price": round(new_low.price, 6),
        "ma_date": pd.Timestamp(frame.loc[len(frame) - 1, "date"]).date().isoformat(),
        "ma_value": round(ma_value, 6),
        "ratio": round(ratio, 6),
        "ratio_low": round(cfg.ratio_low, 6),
        "ratio_high": round(cfg.ratio_high, 6),
        "target_ratio": round(cfg.target_ratio, 6),
        "last_signal_date": (
            pd.Timestamp(frame.loc[engine.last_signal_idx, "date"]).date().isoformat()
            if engine.last_signal_idx is not None
            else ""
        ),
        "signal_count": int(engine.signal_count),
        "signal_dates": ";".join(signal_dates),
        "is_signal": bool(is_signal),
    }
    return row, None


def run_scan(cfg: Config) -> None:
    data = pd.read_parquet(cfg.data)
    data = normalize_input_columns(data)
    listing_dates = derive_listing_dates(data)

    rows: List[Dict[str, object]] = []
    skipped: List[Dict[str, object]] = []

    for code, g in data.groupby("code", sort=False):
        listing_date = pd.Timestamp(listing_dates.get(code, g["date"].min()))
        row, reason = scan_one_code(code=code, df_code_daily=g, listing_date=listing_date, cfg=cfg)
        if row is not None:
            rows.append(row)
        else:
            skipped.append({"code": code, "reason": reason or "unknown"})

    raw_df = pd.DataFrame(rows)
    if not raw_df.empty:
        raw_df = raw_df.sort_values(["ma_date", "code"]).reset_index(drop=True)
    signals_df = raw_df[raw_df["is_signal"]].copy() if not raw_df.empty else raw_df
    if not signals_df.empty:
        signals_df = signals_df.sort_values(["ma_date", "code"]).reset_index(drop=True)

    out_path = Path(cfg.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    raw_out = out_path.with_name(f"{out_path.stem}_raw.csv")
    raw_df.to_csv(raw_out, index=False)
    signals_df.to_csv(out_path, index=False)

    skipped_df = pd.DataFrame(skipped)
    skipped_summary = (
        skipped_df.groupby("reason", dropna=False).size().reset_index(name="count")
        if not skipped_df.empty
        else pd.DataFrame(columns=["reason", "count"])
    )
    skipped_out = out_path.with_name(f"{out_path.stem}_skipped_summary.csv")
    skipped_summary.to_csv(skipped_out, index=False)

    summary = pd.DataFrame(
        [
            {
                "codes_total": int(data["code"].nunique()),
                "codes_with_event": int(len(raw_df)),
                "codes_with_signal": int(signals_df["code"].nunique()) if not signals_df.empty else 0,
                "signal_rows": int(len(signals_df)),
                "signal_rate_over_event": round(
                    float(len(signals_df) / len(raw_df)) if len(raw_df) > 0 else 0.0,
                    6,
                ),
                "avg_ratio_signal": float(signals_df["ratio"].mean()) if not signals_df.empty else np.nan,
            }
        ]
    )
    summary_out = out_path.with_name(f"{out_path.stem}_summary.csv")
    summary.to_csv(summary_out, index=False)

    print(f"Saved event rows: {raw_out}")
    print(f"Saved signal rows: {out_path}")
    print(f"Saved skipped summary: {skipped_out}")
    print(f"Saved summary: {summary_out}")
    print(summary.to_string(index=False))


def main() -> None:
    cfg = parse_args()
    run_scan(cfg)


if __name__ == "__main__":
    main()
