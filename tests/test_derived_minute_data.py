import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from 派生分钟数据 import 聚合到30和60分钟


def make_15m_day() -> pd.DataFrame:
    times = [
        "09:45:00",
        "10:00:00",
        "10:15:00",
        "10:30:00",
        "10:45:00",
        "11:00:00",
        "11:15:00",
        "11:30:00",
        "13:15:00",
        "13:30:00",
        "13:45:00",
        "14:00:00",
        "14:15:00",
        "14:30:00",
        "14:45:00",
        "15:00:00",
    ]
    rows = []
    for i, tm in enumerate(times, start=1):
        rows.append(
            {
                "stock_code": "000001",
                "stock_name": "平安银行",
                "date": "2026-03-06",
                "time": tm,
                "open": float(10 + i),
                "high": float(20 + i),
                "low": float(5 + i),
                "close": float(11 + i),
                "volume": float(100 * i),
                "amount": float(1000 * i),
            }
        )
    return pd.DataFrame(rows)


def test_aggregate_to_30min_uses_two_bars_per_group():
    df = make_15m_day()
    result = 聚合到30和60分钟(df)[30]

    assert result["time"].tolist() == [
        "10:00:00",
        "10:30:00",
        "11:00:00",
        "11:30:00",
        "13:30:00",
        "14:00:00",
        "14:30:00",
        "15:00:00",
    ]

    first = result.iloc[0]
    assert first["stock_code"] == "000001"
    assert first["stock_name"] == "平安银行"
    assert first["open"] == 11.0
    assert first["high"] == 22.0
    assert first["low"] == 6.0
    assert first["close"] == 13.0
    assert first["volume"] == 300.0
    assert first["amount"] == 3000.0


def test_aggregate_to_60min_uses_four_bars_per_group():
    df = make_15m_day()
    result = 聚合到30和60分钟(df)[60]

    assert result["time"].tolist() == [
        "10:30:00",
        "11:30:00",
        "14:00:00",
        "15:00:00",
    ]

    first = result.iloc[0]
    assert first["open"] == 11.0
    assert first["high"] == 24.0
    assert first["low"] == 6.0
    assert first["close"] == 15.0
    assert first["volume"] == 1000.0
    assert first["amount"] == 10000.0

    last = result.iloc[-1]
    assert last["open"] == 23.0
    assert last["high"] == 36.0
    assert last["low"] == 18.0
    assert last["close"] == 27.0
    assert last["volume"] == 5800.0
    assert last["amount"] == 58000.0
