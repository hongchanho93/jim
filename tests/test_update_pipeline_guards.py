import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from 智能更新 import 规范化查询日期 as 日线规范化查询日期
from 智能更新上证指数 import 转日线格式 as 上证指数转日线格式
from 智能更新上证指数 import 转分钟格式 as 上证指数转分钟格式
from 智能更新上证指数 import 聚合日线为月线 as 上证指数聚合月线
from 自动调度更新 import 分钟线依赖日线已变化
from 自动调度更新 import 已达到目标日期, 读取Parquet最大日期
from 更新基本信息 import 整理基本信息快照


def test_daily_query_date_normalizes_timestamp_and_datetime_string():
    assert 日线规范化查询日期(pd.Timestamp("2026-03-26 00:00:00")) == "2026-03-26"
    assert 日线规范化查询日期("2026-03-26 00:00:00") == "2026-03-26"


def test_scheduler_reads_max_date_from_timestamp_parquet(tmp_path):
    path = tmp_path / "daily.parquet"
    pd.DataFrame(
        {
            "stock_code": ["000001", "000001"],
            "date": pd.to_datetime(["2026-03-25", "2026-03-26"]),
        }
    ).to_parquet(path, index=False)

    actual = 读取Parquet最大日期(path)

    assert actual == "2026-03-26"
    assert not 已达到目标日期(actual, "2026-03-27")


def test_scheduler_reads_max_date_from_string_parquet(tmp_path):
    path = tmp_path / "m15.parquet"
    pd.DataFrame(
        {
            "stock_code": ["000001", "000001"],
            "date": ["2026-03-26", "2026-03-27"],
            "time": ["15:00:00", "15:00:00"],
        }
    ).to_parquet(path, index=False)

    actual = 读取Parquet最大日期(path)

    assert actual == "2026-03-27"
    assert 已达到目标日期(actual, "2026-03-27")


def test_m15_should_rerun_when_daily_snapshot_changes():
    old_daily_snapshot = {"exists": True, "size": 100, "mtime": 10}
    new_daily_snapshot = {"exists": True, "size": 200, "mtime": 20}
    state = {"source_daily_snapshot": old_daily_snapshot}

    assert 分钟线依赖日线已变化(state, new_daily_snapshot)
    assert not 分钟线依赖日线已变化(state, old_daily_snapshot)


def test_basic_snapshot_standardizes_market_cap_fields():
    raw = pd.DataFrame(
        [
            {
                "stock_code": "000001",
                "stock_name": "平安银行",
                "date": "2026-04-08",
                "close": "10.0",
                "change_pct": "1.5",
                "change_amount": "0.15",
                "volume": "100000",
                "amount": "1000000",
                "amplitude": "3.2",
                "high": "10.2",
                "low": "9.8",
                "open": "9.9",
                "prev_close": "9.85",
                "turnover": "2.5",
                "total_market_cap": "200000000000",
                "float_market_cap": "150000000000",
                "total_shares": "20000000000",
                "float_shares": "15000000000",
            }
        ]
    )

    actual = 整理基本信息快照(raw, "2026-04-08")

    assert actual.columns.tolist() == [
        "stock_code",
        "stock_name",
        "date",
        "close",
        "change_pct",
        "change_amount",
        "volume",
        "amount",
        "amplitude",
        "high",
        "low",
        "open",
        "prev_close",
        "turnover",
        "total_market_cap",
        "float_market_cap",
        "total_shares",
        "float_shares",
    ]
    row = actual.iloc[0]
    assert row["stock_code"] == "000001"
    assert row["stock_name"] == "平安银行"
    assert row["date"] == "2026-04-08"
    assert row["total_market_cap"] == 200000000000
    assert row["float_market_cap"] == 150000000000
    assert row["total_shares"] == 20000000000
    assert row["float_shares"] == 15000000000


def test_index_daily_update_standardizes_baostock_rows():
    actual = 上证指数转日线格式(
        rows=[
            ["2026-03-26", "4131.1234", "4170.5678", "4120.1111", "4160.9999", "123456789", "987654321.0000", "1.234567"],
        ],
        fields=["date", "open", "high", "low", "close", "volume", "amount", "turn"],
    )

    assert actual.columns.tolist() == [
        "index_code",
        "index_name",
        "date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "amount",
        "turnover",
    ]
    row = actual.iloc[0]
    assert row["index_code"] == "sh.000001"
    assert row["index_name"] == "上证指数"
    assert row["date"] == "2026-03-26"
    assert row["open"] == 4131.1234
    assert row["close"] == 4160.9999
    assert row["turnover"] == 1.234567


def test_index_minute_update_standardizes_baostock_rows():
    actual = 上证指数转分钟格式(
        rows=[
            ["2026-03-26", "20260326150000000", "sh.000001", "4131.1234", "4170.5678", "4120.1111", "4160.9999", "123456789", "987654321.0000"],
        ],
        fields=["date", "time", "code", "open", "high", "low", "close", "volume", "amount"],
    )

    assert actual.columns.tolist() == [
        "index_code",
        "index_name",
        "date",
        "time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "amount",
    ]
    row = actual.iloc[0]
    assert row["date"] == "2026-03-26"
    assert row["time"] == "15:00:00"
    assert row["open"] == 4131.1234
    assert row["amount"] == 987654321.0


def test_monthly_aggregation_uses_daily_rows_to_build_current_month_bar():
    daily = pd.DataFrame(
        {
            "index_code": ["sh.000001", "sh.000001", "sh.000001"],
            "index_name": ["上证指数", "上证指数", "上证指数"],
            "date": ["2026-03-03", "2026-03-17", "2026-03-27"],
            "open": [10.0, 11.0, 12.0],
            "high": [10.5, 11.8, 12.6],
            "low": [9.8, 10.9, 11.7],
            "close": [10.2, 11.5, 12.1],
            "volume": [100, 200, 300],
            "amount": [1000, 2200, 3600],
            "outstanding_share": [5000, 5000, 5000],
            "turnover": [0.1, 0.2, 0.3],
        }
    )

    actual = 上证指数聚合月线(daily)

    assert len(actual) == 1
    row = actual.iloc[0]
    assert row["date"] == "2026-03-27"
    assert row["open"] == 10.0
    assert row["high"] == 12.6
    assert row["low"] == 9.8
    assert row["close"] == 12.1
    assert row["volume"] == 600
    assert row["amount"] == 6800
    assert row["turnover"] == 0.6


def test_replace_current_month_monthly_row_removes_stale_partial_bar():
    monthly = pd.DataFrame(
        {
            "stock_code": ["000001", "000001"],
            "stock_name": ["平安银行", "平安银行"],
            "date": ["2026-02-27", "2026-03-26"],
            "open": [9.0, 10.0],
            "high": [9.8, 12.0],
            "low": [8.7, 9.9],
            "close": [9.5, 11.8],
            "volume": [1000, 2000],
            "amount": [9000, 21000],
            "outstanding_share": [5000, 5000],
            "turnover": [0.5, 0.7],
        }
    )
    replacement = pd.DataFrame(
        {
            "stock_code": ["000001"],
            "stock_name": ["平安银行"],
            "date": ["2026-03-27"],
            "open": [10.0],
            "high": [12.6],
            "low": [9.8],
            "close": [12.1],
            "volume": [2600],
            "amount": [24600],
            "outstanding_share": [5000],
            "turnover": [0.8],
        }
    )

    actual = 替换当月月线(monthly, replacement, "2026-03-27").sort_values("date").reset_index(drop=True)

    assert actual["date"].tolist() == ["2026-02-27", "2026-03-27"]
