import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from 智能更新15分钟 import _计算需更新股票


def test_pending_list_uses_last_traded_day_for_minute_target():
    daily_latest = pd.DataFrame(
        [
            {
                "stock_code": "603056",
                "stock_name": "德邦股份",
                "daily_last": "2026-03-17",
                "minute_target_last": "2026-01-20",
            },
            {
                "stock_code": "300142",
                "stock_name": "沃森生物",
                "daily_last": "2026-03-17",
                "minute_target_last": "2026-03-16",
            },
            {
                "stock_code": "688693",
                "stock_name": "锴威特",
                "daily_last": "2026-03-17",
                "minute_target_last": "2026-03-13",
            },
        ]
    )
    m15_latest = pd.DataFrame(
        [
            {"stock_code": "603056", "stock_name": "德邦股份", "m15_last": "2026-01-20"},
            {"stock_code": "300142", "stock_name": "沃森生物", "m15_last": "2026-03-16"},
            {"stock_code": "688693", "stock_name": "锴威特", "m15_last": "2026-03-13"},
        ]
    )

    need = _计算需更新股票(daily_latest, m15_latest, "2026-03-17")

    assert need.empty


def test_pending_list_still_requires_real_gap():
    daily_latest = pd.DataFrame(
        [
            {
                "stock_code": "002569",
                "stock_name": "ST步森",
                "daily_last": "2026-03-16",
                "minute_target_last": "2026-03-13",
            },
            {
                "stock_code": "300385",
                "stock_name": "雪浪环境",
                "daily_last": "2026-03-17",
                "minute_target_last": "2026-03-16",
            },
        ]
    )
    m15_latest = pd.DataFrame(
        [
            {"stock_code": "002569", "stock_name": "ST步森", "m15_last": "2026-03-12"},
            {"stock_code": "300385", "stock_name": "雪浪环境", "m15_last": "2026-03-16"},
        ]
    )

    need = _计算需更新股票(daily_latest, m15_latest, "2026-03-17")

    assert need["stock_code"].tolist() == ["002569"]
    assert need.iloc[0]["minute_target_last"] == "2026-03-13"
