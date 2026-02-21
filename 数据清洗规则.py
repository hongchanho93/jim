"""三周期数据清洗共享规则。

说明：
- turnover 统一为“百分比数值”，即 0.35 表示 0.35%。
- 停牌/零成交记录保留，仅做标准化。
- 提供日线/月线/15分钟可复用清洗函数，及15分钟文件级统计函数。
"""

from __future__ import annotations

import math
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow.parquet as pq


日线月线列 = [
    "stock_code",
    "stock_name",
    "date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
    "outstanding_share",
    "turnover",
]

分钟列 = [
    "stock_code",
    "stock_name",
    "date",
    "time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
]

价格列 = ["open", "high", "low", "close"]

预期分钟时间 = {
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
}


def _补齐缺失列(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    out = df.copy()
    for col in cols:
        if col not in out.columns:
            out[col] = np.nan
    return out[cols]


def _标准化股票代码(s: pd.Series) -> pd.Series:
    return s.fillna("").astype(str).str.strip().str.extract(r"(\d{6})", expand=False).fillna("")


def _标准化日期(s: pd.Series) -> pd.Series:
    dt = pd.to_datetime(s, errors="coerce", format="mixed")
    return dt.dt.strftime("%Y-%m-%d")


def _标准化时间(s: pd.Series) -> pd.Series:
    x = s.fillna("").astype(str).str.strip()

    # BaoStock 15分钟返回形如 20260213150000000
    mask_long = x.str.fullmatch(r"\d{14,17}", na=False)
    if mask_long.any():
        xx = x.loc[mask_long]
        x.loc[mask_long] = xx.str.slice(8, 10) + ":" + xx.str.slice(10, 12) + ":" + xx.str.slice(12, 14)

    # HH:MM 补秒
    mask_hhmm = x.str.fullmatch(r"\d{2}:\d{2}", na=False)
    if mask_hhmm.any():
        x.loc[mask_hhmm] = x.loc[mask_hhmm] + ":00"

    # 其余非法时间置空，后续删除
    mask_ok = x.str.fullmatch(r"\d{2}:\d{2}:\d{2}", na=False)
    x.loc[~mask_ok] = ""
    return x


def _规范化股票名称(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["stock_name"] = out["stock_name"].fillna("").astype(str).str.strip()

    cand = out[["stock_code", "stock_name"]].copy()
    cand = cand[cand["stock_code"] != ""]
    cand = cand[cand["stock_name"] != ""]
    if cand.empty:
        return out

    freq = cand.groupby(["stock_code", "stock_name"], as_index=False).size()
    freq = freq.rename(columns={"size": "cnt"})

    pos = cand.reset_index().groupby(["stock_code", "stock_name"], as_index=False)["index"].max()
    freq = freq.merge(pos, on=["stock_code", "stock_name"], how="left")

    best = (
        freq.sort_values(["stock_code", "cnt", "index"], ascending=[True, False, False])
        .drop_duplicates(subset=["stock_code"], keep="first")
        .loc[:, ["stock_code", "stock_name"]]
    )
    name_map = dict(zip(best["stock_code"], best["stock_name"]))
    out["stock_name"] = out["stock_code"].map(name_map).fillna(out["stock_name"])
    return out


def _转数值(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    out = df.copy()
    for c in cols:
        out[c] = pd.to_numeric(out[c], errors="coerce")
    return out


def _修复价格逻辑(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    out = df.copy()
    info: dict[str, int] = {}

    for c in 价格列:
        out[c] = pd.to_numeric(out[c], errors="coerce")

    close_pos = out["close"] > 0

    # 开高低异常为0/空且close有效，则回填close
    fix_from_close = pd.Series(False, index=out.index)
    for c in ["open", "high", "low"]:
        mask = (out[c].isna() | (out[c] <= 0)) & close_pos
        info[f"fill_{c}_from_close"] = int(mask.sum())
        if mask.any():
            out.loc[mask, c] = out.loc[mask, "close"]
            fix_from_close = fix_from_close | mask

    # close异常且open有效，则回填open
    mask_close = (out["close"].isna() | (out["close"] <= 0)) & (out["open"] > 0)
    info["fill_close_from_open"] = int(mask_close.sum())
    if mask_close.any():
        out.loc[mask_close, "close"] = out.loc[mask_close, "open"]

    mx = out[价格列].max(axis=1, skipna=True)
    mn = out[价格列].min(axis=1, skipna=True)

    mask_high = out["high"].isna() | (out["high"] < mx)
    info["fix_high_bound"] = int(mask_high.sum())
    if mask_high.any():
        out.loc[mask_high, "high"] = mx.loc[mask_high]

    mask_low = out["low"].isna() | (out["low"] > mn)
    info["fix_low_bound"] = int(mask_low.sum())
    if mask_low.any():
        out.loc[mask_low, "low"] = mn.loc[mask_low]

    return out, info


def _归一化换手率到百分比(df: pd.DataFrame, date_col: str = "date") -> tuple[pd.DataFrame, dict[str, Any]]:
    """将 turnover 统一为百分比数值（0.35 表示 0.35%）。"""

    out = df.copy()
    out = _转数值(out, ["volume", "turnover", "outstanding_share"])

    vol = out["volume"]
    turn = out["turnover"]
    old_turn = turn.copy()
    oss = out["outstanding_share"]

    valid = (vol > 0) & (turn > 0) & (oss > 0)
    implied_pct = pd.Series(np.nan, index=out.index, dtype="float64")
    implied_pct.loc[valid] = vol.loc[valid] / oss.loc[valid] * 100.0

    ratio = pd.Series(np.nan, index=out.index, dtype="float64")
    ratio.loc[valid] = turn.loc[valid] / implied_pct.loc[valid]

    ratio_valid = ratio[ratio.notna() & np.isfinite(ratio) & (ratio > 0)]
    global_scale = float(ratio_valid.median()) if not ratio_valid.empty else 1.0
    if math.isnan(global_scale) or global_scale <= 0:
        global_scale = 1.0

    date_scale = (
        ratio_valid.groupby(out.loc[ratio_valid.index, date_col]).median() if not ratio_valid.empty else pd.Series(dtype=float)
    )
    fallback_scale = out[date_col].map(date_scale).fillna(global_scale)

    # ratio≈0.01 认为是小数比例，应*100转百分比；ratio≈1保持不变
    mask_scale_up = (turn > 0) & (
        ((ratio.notna()) & (ratio < 0.1))
        | ((ratio.isna()) & (fallback_scale < 0.1))
    )

    if mask_scale_up.any():
        out.loc[mask_scale_up, "turnover"] = turn.loc[mask_scale_up] * 100.0

    # 标准化无效值
    out["turnover"] = pd.to_numeric(out["turnover"], errors="coerce").fillna(0.0)
    out.loc[out["turnover"] < 0, "turnover"] = 0.0

    # 统一重算流通股本：volume / turnover * 100
    turn2 = out["turnover"]
    vol2 = pd.to_numeric(out["volume"], errors="coerce").fillna(0.0)
    calc = pd.Series(0.0, index=out.index, dtype="float64")
    good = (vol2 > 0) & (turn2 > 0)
    calc.loc[good] = vol2.loc[good] / turn2.loc[good] * 100.0
    out["outstanding_share"] = calc

    # 停牌/无成交时，用历史最近有效流通股本前向填充
    out = out.sort_values(["stock_code", date_col], kind="mergesort").reset_index(drop=True)
    filled = (
        out["outstanding_share"]
        .replace(0, np.nan)
        .groupby(out["stock_code"])
        .ffill()
        .fillna(0.0)
    )
    out["outstanding_share"] = filled

    info = {
        "turnover_scaled_up_rows": int(mask_scale_up.sum()),
        "turnover_scale_global": global_scale,
        "turnover_before_lt_0_1_rows": int(((old_turn > 0) & (old_turn < 0.1)).sum()),
        "turnover_after_lt_0_1_rows": int(((out["turnover"] > 0) & (out["turnover"] < 0.1)).sum()),
    }
    return out, info


def _标准化停牌字段(df: pd.DataFrame, with_turnover: bool) -> tuple[pd.DataFrame, dict[str, int]]:
    out = df.copy()
    info: dict[str, int] = {}

    for c in ["volume", "amount"]:
        out[c] = pd.to_numeric(out[c], errors="coerce")

    vol_nan = out["volume"].isna()
    amt_nan = out["amount"].isna()
    info["fill_volume_nan_to_zero"] = int(vol_nan.sum())
    info["fill_amount_nan_to_zero"] = int(amt_nan.sum())
    out.loc[vol_nan, "volume"] = 0.0
    out.loc[amt_nan, "amount"] = 0.0

    vol_neg = out["volume"] < 0
    amt_neg = out["amount"] < 0
    info["clip_volume_negative"] = int(vol_neg.sum())
    info["clip_amount_negative"] = int(amt_neg.sum())
    out.loc[vol_neg, "volume"] = 0.0
    out.loc[amt_neg, "amount"] = 0.0

    zero_vol = out["volume"] <= 0
    # 无成交额统一归零
    out.loc[zero_vol, "amount"] = 0.0

    if with_turnover and "turnover" in out.columns:
        out["turnover"] = pd.to_numeric(out["turnover"], errors="coerce").fillna(0.0)
        out.loc[out["turnover"] < 0, "turnover"] = 0.0
        out.loc[zero_vol, "turnover"] = 0.0

    return out, info


def _基础质量统计(df: pd.DataFrame, keys: list[str], with_turnover: bool, with_time: bool) -> dict[str, Any]:
    out: dict[str, Any] = {}
    out["rows"] = int(len(df))
    out["stocks"] = int(df["stock_code"].nunique()) if "stock_code" in df.columns else 0
    out["date_min"] = str(df["date"].min()) if "date" in df.columns and not df.empty else ""
    out["date_max"] = str(df["date"].max()) if "date" in df.columns and not df.empty else ""

    out["duplicate_key_rows"] = int(df.duplicated(subset=keys, keep=False).sum()) if len(df) else 0
    out["invalid_stock_code_rows"] = int((~df["stock_code"].astype(str).str.fullmatch(r"\d{6}", na=False)).sum())

    if len(df):
        non_pos = (df[价格列] <= 0).any(axis=1)
        out["price_non_positive_rows"] = int(non_pos.sum())
        out["high_logic_bad_rows"] = int((df["high"] < df[["open", "close", "low"]].max(axis=1)).sum())
        out["low_logic_bad_rows"] = int((df["low"] > df[["open", "close", "high"]].min(axis=1)).sum())
        out["volume_zero_rows"] = int((df["volume"] <= 0).sum())
        out["amount_zero_rows"] = int((df["amount"] <= 0).sum())
    else:
        out["price_non_positive_rows"] = 0
        out["high_logic_bad_rows"] = 0
        out["low_logic_bad_rows"] = 0
        out["volume_zero_rows"] = 0
        out["amount_zero_rows"] = 0

    if with_turnover and "turnover" in df.columns:
        t = pd.to_numeric(df["turnover"], errors="coerce")
        out["turnover_non_null_rows"] = int(t.notna().sum())
        out["turnover_positive_rows"] = int((t > 0).sum())
        out["turnover_lt_0_1_rows"] = int(((t > 0) & (t < 0.1)).sum())

    if with_time and "time" in df.columns:
        tm = df["time"].astype(str)
        out["invalid_time_rows"] = int((~tm.str.fullmatch(r"\d{2}:\d{2}:\d{2}", na=False)).sum())
        out["unexpected_time_rows"] = int((~tm.isin(预期分钟时间)).sum())

    return out


def 清洗日线(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    src = _补齐缺失列(df, 日线月线列)

    before = _基础质量统计(
        _转数值(src.assign(stock_code=_标准化股票代码(src["stock_code"]), date=_标准化日期(src["date"])), ["open", "high", "low", "close", "volume", "amount", "turnover"]),
        keys=["stock_code", "date"],
        with_turnover=True,
        with_time=False,
    )

    out = src.copy()
    out["stock_code"] = _标准化股票代码(out["stock_code"])
    out["date"] = _标准化日期(out["date"])
    out = out[(out["stock_code"] != "") & out["date"].notna()].copy()

    out = _规范化股票名称(out)
    out = _转数值(out, ["open", "high", "low", "close", "volume", "amount", "outstanding_share", "turnover"])

    out, info_stop = _标准化停牌字段(out, with_turnover=True)
    out, info_turn = _归一化换手率到百分比(out, date_col="date")
    out, info_price = _修复价格逻辑(out)

    out = out.drop_duplicates(subset=["stock_code", "date"], keep="last")
    out = out.sort_values(["stock_code", "date"], kind="mergesort").reset_index(drop=True)
    out = _补齐缺失列(out, 日线月线列)

    after = _基础质量统计(out, keys=["stock_code", "date"], with_turnover=True, with_time=False)

    report: dict[str, Any] = {
        "before": before,
        "after": after,
        "fixes": {**info_stop, **info_turn, **info_price},
    }
    return out, report


def 清洗月线(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    src = _补齐缺失列(df, 日线月线列)

    before = _基础质量统计(
        _转数值(src.assign(stock_code=_标准化股票代码(src["stock_code"]), date=_标准化日期(src["date"])), ["open", "high", "low", "close", "volume", "amount", "turnover"]),
        keys=["stock_code", "date"],
        with_turnover=True,
        with_time=False,
    )

    out = src.copy()
    out["stock_code"] = _标准化股票代码(out["stock_code"])
    out["date"] = _标准化日期(out["date"])
    out = out[(out["stock_code"] != "") & out["date"].notna()].copy()

    out = _规范化股票名称(out)
    out = _转数值(out, ["open", "high", "low", "close", "volume", "amount", "outstanding_share", "turnover"])

    out, info_stop = _标准化停牌字段(out, with_turnover=True)
    out, info_turn = _归一化换手率到百分比(out, date_col="date")
    out, info_price = _修复价格逻辑(out)

    out = out.drop_duplicates(subset=["stock_code", "date"], keep="last")
    out = out.sort_values(["stock_code", "date"], kind="mergesort").reset_index(drop=True)
    out = _补齐缺失列(out, 日线月线列)

    after = _基础质量统计(out, keys=["stock_code", "date"], with_turnover=True, with_time=False)

    report: dict[str, Any] = {
        "before": before,
        "after": after,
        "fixes": {**info_stop, **info_turn, **info_price},
    }
    return out, report


def 清洗15分钟(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    src = _补齐缺失列(df, 分钟列)

    base = src.copy()
    base["stock_code"] = _标准化股票代码(base["stock_code"])
    base["date"] = _标准化日期(base["date"])
    base["time"] = _标准化时间(base["time"])
    base = _转数值(base, ["open", "high", "low", "close", "volume", "amount"])
    before = _基础质量统计(base, keys=["stock_code", "date", "time"], with_turnover=False, with_time=True)

    out = base
    out = out[(out["stock_code"] != "") & out["date"].notna() & (out["time"] != "")].copy()
    out = _规范化股票名称(out)

    out, info_stop = _标准化停牌字段(out, with_turnover=False)

    out = out.sort_values(["stock_code", "date", "time"], kind="mergesort").reset_index(drop=True)
    all_zero_bar = (
        (out["open"] <= 0)
        & (out["high"] <= 0)
        & (out["low"] <= 0)
        & (out["close"] <= 0)
        & (out["volume"] <= 0)
    )

    # 全零停牌K线：价格回填到前一条有效close（若存在），否则保留0
    prev_close = out.groupby("stock_code")["close"].shift(1)
    fill_close = prev_close.where(prev_close > 0)
    for c in 价格列:
        mask = all_zero_bar & fill_close.notna()
        out.loc[mask, c] = fill_close.loc[mask]

    out, info_price = _修复价格逻辑(out)

    out = out.drop_duplicates(subset=["stock_code", "date", "time"], keep="last")
    out = out.sort_values(["stock_code", "date", "time"], kind="mergesort").reset_index(drop=True)
    out = _补齐缺失列(out, 分钟列)

    after = _基础质量统计(out, keys=["stock_code", "date", "time"], with_turnover=False, with_time=True)
    report: dict[str, Any] = {
        "before": before,
        "after": after,
        "fixes": {
            **info_stop,
            **info_price,
            "all_zero_bar_rows": int(all_zero_bar.sum()),
        },
    }
    return out, report


def 清洗15分钟分块(df: pd.DataFrame) -> pd.DataFrame:
    """用于大文件分块清洗，不做跨分块去重。"""
    cleaned, _ = 清洗15分钟(df)
    return cleaned


def 原子写入Parquet(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    if tmp.exists():
        tmp.unlink()
    df.to_parquet(tmp, engine="pyarrow", compression="snappy", index=False)
    tmp.replace(path)


def 汇总15分钟文件(path: Path, batch_size: int = 600_000) -> dict[str, Any]:
    """基于流式扫描的15分钟文件体检统计。"""
    pf = pq.ParquetFile(path)

    rows = 0
    stocks: set[str] = set()
    min_date: str | None = None
    max_date: str | None = None

    invalid_code = 0
    invalid_time = 0
    unexpected_time = 0
    price_non_pos = 0
    dup_adjacent = 0
    sort_violation = 0

    prev_key: str | None = None

    for batch in pf.iter_batches(
        columns=["stock_code", "date", "time", "open", "high", "low", "close", "volume", "amount"],
        batch_size=batch_size,
    ):
        df = batch.to_pandas()
        if df.empty:
            continue

        rows += len(df)
        sc = df["stock_code"].astype(str).str.strip()
        dt = df["date"].astype(str).str.strip()
        tm = df["time"].astype(str).str.strip()

        stocks.update(sc.unique().tolist())
        invalid_code += int((~sc.str.fullmatch(r"\d{6}", na=False)).sum())
        invalid_time += int((~tm.str.fullmatch(r"\d{2}:\d{2}:\d{2}", na=False)).sum())
        unexpected_time += int((~tm.isin(预期分钟时间)).sum())

        bmin = dt.min()
        bmax = dt.max()
        min_date = bmin if min_date is None or bmin < min_date else min_date
        max_date = bmax if max_date is None or bmax > max_date else max_date

        for c in ["open", "high", "low", "close"]:
            df[c] = pd.to_numeric(df[c], errors="coerce")

        price_non_pos += int((df[价格列] <= 0).any(axis=1).sum())

        key = (sc + "|" + dt + "|" + tm).to_numpy()
        if prev_key is not None:
            if key[0] < prev_key:
                sort_violation += 1
            if key[0] == prev_key:
                dup_adjacent += 1
        if len(key) > 1:
            sort_violation += int((key[1:] < key[:-1]).sum())
            dup_adjacent += int((key[1:] == key[:-1]).sum())
        prev_key = key[-1]

    return {
        "rows": rows,
        "stocks": len(stocks),
        "date_min": min_date or "",
        "date_max": max_date or "",
        "invalid_stock_code_rows": invalid_code,
        "invalid_time_rows": invalid_time,
        "unexpected_time_rows": unexpected_time,
        "price_non_positive_rows": price_non_pos,
        "adjacent_duplicate_key_rows": dup_adjacent,
        "sort_violations": sort_violation,
    }


def 打印报告(title: str, report: dict[str, Any]) -> None:
    print("\n" + "=" * 88)
    print(title)
    print("=" * 88)

    before = report.get("before", {})
    after = report.get("after", {})
    fixes = report.get("fixes", {})

    print("[Before]")
    for k in sorted(before.keys()):
        print(f"  {k}: {before[k]}")

    print("\n[After]")
    for k in sorted(after.keys()):
        print(f"  {k}: {after[k]}")

    print("\n[Fixes]")
    for k in sorted(fixes.keys()):
        print(f"  {k}: {fixes[k]}")
