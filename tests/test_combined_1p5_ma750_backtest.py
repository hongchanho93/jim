import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from combined_1p5_ma750_backtest import (
    build_part2_candidates,
    evaluate_forward_returns,
    find_part1_match_date,
)


def test_build_part2_candidates_pick_first_resonance_and_no_reuse():
    day_state = pd.DataFrame(
        {
            "date": [
                "2026-01-02",
                "2026-01-03",
                "2026-01-04",
                "2026-01-05",
                "2026-01-07",
                "2026-01-08",
                "2026-01-09",
            ],
            "date_dt": pd.to_datetime(
                [
                    "2026-01-02",
                    "2026-01-03",
                    "2026-01-04",
                    "2026-01-05",
                    "2026-01-07",
                    "2026-01-08",
                    "2026-01-09",
                ]
            ),
            "resonance": [True, True, False, False, True, False, False],
            "bull": [False, False, True, False, False, True, True],
        }
    )
    events, reason = build_part2_candidates(
        day_state=day_state,
        signal_start=pd.Timestamp("2026-01-01"),
        signal_end=pd.Timestamp("2026-01-31"),
        month_window=1,
    )
    assert reason is None
    assert len(events) == 2
    assert events[0]["resonance_date"] == pd.Timestamp("2026-01-02")
    assert events[0]["bull_start_date"] == pd.Timestamp("2026-01-04")
    # The second phase cannot reuse old resonance before previous phase end.
    assert events[1]["resonance_date"] == pd.Timestamp("2026-01-07")
    assert events[1]["bull_start_date"] == pd.Timestamp("2026-01-08")


def test_find_part1_match_date_supports_both_directions():
    resonance_date = pd.Timestamp("2026-01-15")
    part1_dates = [
        pd.Timestamp("2025-12-28"),  # before resonance, in 1 month
        pd.Timestamp("2026-02-01"),  # after resonance, in 1 month and closer
        pd.Timestamp("2026-03-01"),  # out of 1 month
    ]
    matched = find_part1_match_date(part1_dates, resonance_date, month_window=1)
    assert matched is not None
    date, gap = matched
    assert date == pd.Timestamp("2026-02-01")
    assert gap == 17


def test_evaluate_forward_returns_uses_future_high_and_window_validity():
    daily_price = pd.DataFrame(
        {
            "date_dt": pd.to_datetime(
                ["2026-01-05", "2026-01-06", "2026-01-07", "2026-01-08", "2026-01-09"]
            ),
            "high": [10.2, 11.0, 10.5, 12.0, 11.2],
            "close": [10.0, 10.8, 10.3, 11.5, 11.0],
        }
    )
    base_close, returns, valid = evaluate_forward_returns(
        daily_price=daily_price,
        bull_start_date=pd.Timestamp("2026-01-05"),
        windows=(2, 4),
    )
    assert base_close == 10.0
    # Window=2 uses highs on 01-06 and 01-07, max=11.0 -> +10%.
    assert round(returns[2], 6) == 0.1
    assert valid[2] is True
    # Window=4 has exactly enough future bars (01-06..01-09), max=12.0 -> +20%.
    assert round(returns[4], 6) == 0.2
    assert valid[4] is True

    _, returns_short, valid_short = evaluate_forward_returns(
        daily_price=daily_price,
        bull_start_date=pd.Timestamp("2026-01-08"),
        windows=(2,),
    )
    assert pd.isna(returns_short[2])
    assert valid_short[2] is False
