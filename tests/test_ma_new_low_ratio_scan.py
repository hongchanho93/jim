import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ma_new_low_ratio_scan import (
    Config,
    collect_causal_signal_indices_all_events,
    collect_part1_signal_dates_for_code,
    detect_fractal_swings,
    find_latest_rally_event,
    find_new_low_since_peak,
    run_causal_engine,
    scan_one_code,
)


def make_config() -> Config:
    return Config(
        data="dummy.parquet",
        out="outputs/ma_new_low_ratio_signals.csv",
        split_date=pd.Timestamp("2019-01-01"),
        pre_window=32,
        post_window=64,
        fractal_n=3,
        min_multiple=2.5,
        ratio_low=1.45,
        ratio_high=1.55,
        target_ratio=1.50,
        weekly_rule="W-FRI",
        monthly_rule="ME",
    )


def test_detect_fractal_swings_basic():
    df = pd.DataFrame(
        {
            "date": pd.date_range("2024-01-01", periods=11, freq="D"),
            "open": [6, 5, 4, 5, 6, 5, 3, 5, 6, 5, 4],
            "high": [7, 6, 8, 6, 6, 6, 9, 6, 6, 6, 5],
            "low": [5, 4, 3, 4, 5, 4, 2, 4, 5, 4, 3],
            "close": [6, 5, 4, 5, 6, 5, 3, 5, 6, 5, 4],
        }
    )
    bottoms, peaks = detect_fractal_swings(df, n=1)
    assert bottoms == [2, 6]
    assert peaks == [2, 6]


def test_find_latest_rally_event_picks_latest_valid_peak():
    df = pd.DataFrame(
        {
            "date": pd.date_range("2024-01-01", periods=8, freq="D"),
            "open": [5, 4, 8, 7, 3, 4, 6, 8],
            "high": [6, 7, 9, 8, 4, 6, 8, 10],
            "low": [5, 3, 4, 5, 2, 3, 4, 5],
            "close": [5, 4, 8, 7, 3, 4, 6, 8],
        }
    )
    event = find_latest_rally_event(df, bottoms=[1, 4], peaks=[2, 7], min_multiple=2.5)
    assert event is not None
    assert event.bottom_idx == 4
    assert event.peak_idx == 7
    assert round(event.multiple, 4) == 5.0


def test_find_new_low_since_peak():
    df = pd.DataFrame(
        {
            "date": pd.date_range("2024-01-01", periods=6, freq="D"),
            "open": [10, 11, 9, 8, 7, 8],
            "high": [11, 12, 10, 9, 8, 9],
            "low": [9, 10, 8, 7, 6, 7],
            "close": [10, 11, 9, 8, 7, 8],
        }
    )
    p = find_new_low_since_peak(df, peak_idx=1)
    assert p is not None
    assert p.idx == 4
    assert p.price == 6


def test_scan_one_code_weekly_signal():
    cfg = make_config()
    dates = pd.date_range("2020-01-03", periods=70, freq="W-FRI")
    rows = []
    for d in dates:
        rows.append(
            {
                "date": d,
                "code": "sh.600000",
                "open": 150.0,
                "high": 151.0,
                "low": 149.0,
                "close": 150.0,
            }
        )
    df = pd.DataFrame(rows)
    df.loc[10, ["open", "high", "low", "close"]] = [45.0, 50.0, 40.0, 45.0]
    df.loc[20, ["open", "high", "low", "close"]] = [195.0, 220.0, 190.0, 200.0]
    df.loc[30, ["open", "high", "low", "close"]] = [105.0, 115.0, 100.0, 110.0]

    row, reason = scan_one_code(
        code="sh.600000",
        df_code_daily=df,
        listing_date=pd.Timestamp("2021-01-01"),
        cfg=cfg,
    )
    assert reason is None
    assert row is not None
    assert row["group_type"] == "weekly_ma64"
    assert row["is_signal"] is True
    assert 1.45 <= row["ratio"] <= 1.55
    assert row["signal_count"] == 1


def test_scan_one_code_monthly_signal():
    cfg = make_config()
    dates = pd.date_range("2010-01-31", periods=40, freq="ME")
    rows = []
    for d in dates:
        rows.append(
            {
                "date": d,
                "code": "sz.000001",
                "open": 90.0,
                "high": 91.0,
                "low": 89.0,
                "close": 90.0,
            }
        )
    df = pd.DataFrame(rows)
    df.loc[5, ["open", "high", "low", "close"]] = [32.0, 35.0, 30.0, 32.0]
    df.loc[12, ["open", "high", "low", "close"]] = [98.0, 100.0, 95.0, 98.0]
    df.loc[20, ["open", "high", "low", "close"]] = [65.0, 70.0, 60.0, 65.0]

    row, reason = scan_one_code(
        code="sz.000001",
        df_code_daily=df,
        listing_date=pd.Timestamp("2010-01-01"),
        cfg=cfg,
    )
    assert reason is None
    assert row is not None
    assert row["group_type"] == "monthly_ma32"
    assert row["ma_window"] == 32
    assert row["is_signal"] is True
    assert 1.45 <= row["ratio"] <= 1.55
    assert row["signal_count"] == 1


def test_scan_one_code_no_future_peak_not_confirmed():
    cfg = make_config()
    dates = pd.date_range("2020-01-03", periods=70, freq="W-FRI")
    rows = []
    for d in dates:
        rows.append(
            {
                "date": d,
                "code": "sh.600001",
                "open": 120.0,
                "high": 121.0,
                "low": 119.0,
                "close": 120.0,
            }
        )
    df = pd.DataFrame(rows)
    # bottom is valid
    df.loc[10, ["open", "high", "low", "close"]] = [42.0, 45.0, 40.0, 43.0]
    # large peak appears too close to the end; with fractal_n=3 it cannot be confirmed
    df.loc[68, ["open", "high", "low", "close"]] = [180.0, 200.0, 175.0, 190.0]

    row, reason = scan_one_code(
        code="sh.600001",
        df_code_daily=df,
        listing_date=pd.Timestamp("2021-01-01"),
        cfg=cfg,
    )
    assert row is None
    assert reason == "no_rally_event"


def test_run_causal_engine_dedup_and_pick_closest_to_target():
    cfg = Config(
        data="dummy.parquet",
        out="outputs/ma_new_low_ratio_signals.csv",
        split_date=pd.Timestamp("2019-01-01"),
        pre_window=32,
        post_window=64,
        fractal_n=1,
        min_multiple=2.5,
        ratio_low=1.45,
        ratio_high=1.55,
        target_ratio=1.50,
        weekly_rule="W-FRI",
        monthly_rule="ME",
    )
    frame = pd.DataFrame(
        {
            "date": pd.date_range("2024-01-01", periods=13, freq="W-FRI"),
            "open": [10, 9, 4, 8, 7, 11, 11, 11, 11, 11, 11, 11, 11],
            "high": [11, 10, 5, 9, 12, 11, 11, 11, 11, 11, 11, 11, 11],
            "low": [10, 9, 4, 8, 7, 10, 10, 10, 10, 10, 10, 10, 10],
            "close": [10, 9, 4, 8, 7, 11, 11, 11, 11, 11, 11, 11, 11],
            "ma": [10, 10, 10, 10, 10, 14.6, 15.1, 15.4, 15.7, 15.2, 14.9, 14.4, 15.0],
        }
    )
    result = run_causal_engine(frame, cfg)
    # Three signal zones:
    # 1) [5,6,7] -> choose 6 (15.1)
    # 2) [9,10] -> choose 10 (14.9)
    # 3) [12] -> choose 12 (15.0)
    assert result.signal_indices == (6, 10, 12)
    assert result.signal_count == 3
    assert result.last_signal_idx == 12


def test_collect_causal_signal_indices_all_events_single_event():
    cfg = Config(
        data="dummy.parquet",
        out="outputs/ma_new_low_ratio_signals.csv",
        split_date=pd.Timestamp("2019-01-01"),
        pre_window=32,
        post_window=64,
        fractal_n=1,
        min_multiple=2.5,
        ratio_low=1.45,
        ratio_high=1.55,
        target_ratio=1.50,
        weekly_rule="W-FRI",
        monthly_rule="ME",
    )
    frame = pd.DataFrame(
        {
            "date": pd.date_range("2024-01-01", periods=13, freq="W-FRI"),
            "open": [10, 9, 4, 8, 7, 11, 11, 11, 11, 11, 11, 11, 11],
            "high": [11, 10, 5, 9, 12, 11, 11, 11, 11, 11, 11, 11, 11],
            "low": [10, 9, 4, 8, 7, 10, 10, 10, 10, 10, 10, 10, 10],
            "close": [10, 9, 4, 8, 7, 11, 11, 11, 11, 11, 11, 11, 11],
            "ma": [10, 10, 10, 10, 10, 14.6, 15.1, 15.4, 15.7, 15.2, 14.9, 14.4, 15.0],
        }
    )
    indices = collect_causal_signal_indices_all_events(frame, cfg)
    assert indices == (6, 10, 12)


def test_collect_part1_signal_dates_for_code_smoke():
    cfg = Config(
        data="dummy.parquet",
        out="outputs/ma_new_low_ratio_signals.csv",
        split_date=pd.Timestamp("2019-01-01"),
        pre_window=2,
        post_window=2,
        fractal_n=1,
        min_multiple=2.5,
        ratio_low=0.8,
        ratio_high=1.2,
        target_ratio=1.0,
        weekly_rule="W-FRI",
        monthly_rule="ME",
    )

    dates = pd.date_range("2024-01-05", periods=14, freq="W-FRI")
    rows = []
    for idx, d in enumerate(dates):
        open_v = 10.0
        high_v = 11.0
        low_v = 9.5
        close_v = 10.0
        if idx == 2:
            open_v, high_v, low_v, close_v = 4.2, 5.0, 4.0, 4.2
        if idx == 4:
            open_v, high_v, low_v, close_v = 11.5, 12.0, 7.0, 10.8
        rows.append(
            {
                "date": d,
                "code": "sh.600000",
                "open": open_v,
                "high": high_v,
                "low": low_v,
                "close": close_v,
            }
        )
    df = pd.DataFrame(rows)

    signal_dates, reason = collect_part1_signal_dates_for_code(
        code="sh.600000",
        df_code_daily=df,
        listing_date=pd.Timestamp("2021-01-01"),
        cfg=cfg,
    )
    assert reason is None
    assert len(signal_dates) >= 1
