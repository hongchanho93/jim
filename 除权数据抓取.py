from __future__ import annotations

import concurrent.futures
import contextlib
import dataclasses
import datetime as dt
import io
import math
import re
import time
from collections import defaultdict
from pathlib import Path
from typing import Sequence

import akshare as ak
import pandas as pd
import requests


数据目录 = Path("/Users/hongjian/Documents/CODEX项目/股票数据维护/数据")
日线数据路径 = 数据目录 / "全市场历史数据.parquet"
支持前缀 = (
    "000",
    "001",
    "002",
    "003",
    "300",
    "301",
    "600",
    "601",
    "603",
    "605",
    "688",
)
今天 = int(dt.date.today().strftime("%Y%m%d"))


@dataclasses.dataclass(frozen=True)
class CorporateAction:
    ex_date: int
    bonus_per10: float = 0.0
    transfer_per10: float = 0.0
    cash_div_per10: float = 0.0
    rights_per10: float = 0.0
    rights_price: float = 0.0


@dataclasses.dataclass(frozen=True)
class ShareChange:
    change_date: int
    float_shares: int


@dataclasses.dataclass
class FetchStats:
    total_codes: int = 0
    dividend_codes_from_bulk: int = 0
    dividend_codes_from_cninfo: int = 0
    dividend_codes_from_ths: int = 0
    rights_codes: int = 0
    share_change_codes_from_cninfo: int = 0
    share_change_codes_from_em: int = 0
    share_change_codes: int = 0
    share_change_codes_from_local_fallback: int = 0
    codes_without_dividends: int = 0
    codes_without_share_changes: int = 0
    total_dividend_events: int = 0
    total_rights_events: int = 0
    total_share_change_points: int = 0


def log(message: str) -> None:
    print(message, flush=True)


def safe_float(value: object) -> float:
    if value is None:
        return 0.0
    if isinstance(value, float):
        if math.isnan(value):
            return 0.0
        return value
    if isinstance(value, int):
        return float(value)
    if isinstance(value, str):
        cleaned = value.strip().replace(",", "")
        if not cleaned or cleaned in {"--", "nan", "None"}:
            return 0.0
        try:
            return float(cleaned)
        except ValueError:
            return 0.0
    try:
        if pd.isna(value):
            return 0.0
    except TypeError:
        pass
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def to_date_int(value: object) -> int | None:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass
    try:
        ts = pd.to_datetime(value)
    except Exception:
        return None
    if pd.isna(ts):
        return None
    return int(ts.strftime("%Y%m%d"))


def parse_plan_text(text: str) -> tuple[float, float, float]:
    if not text:
        return 0.0, 0.0, 0.0
    normalized = (
        text.replace(" ", "")
        .replace("每10股", "10")
        .replace("每十股", "10")
        .replace("转增", "转")
        .replace("(含税)", "")
        .replace("（含税）", "")
    )
    bonus = 0.0
    transfer = 0.0
    cash = 0.0
    m = re.search(r"10送([0-9.]+)股", normalized)
    if m:
        bonus = safe_float(m.group(1))
    m = re.search(r"10转([0-9.]+)股", normalized)
    if m:
        transfer = safe_float(m.group(1))
    m = re.search(r"10派([0-9.]+)元", normalized)
    if m:
        cash = safe_float(m.group(1))
    return bonus, transfer, cash


def supported_code(code: str) -> bool:
    return code.startswith(支持前缀)


def eastmoney_symbol(code: str) -> str:
    return f"{code}.SH" if code.startswith(("600", "601", "603", "605", "688")) else f"{code}.SZ"


def load_supported_universe() -> list[str]:
    df = pd.read_parquet(日线数据路径, columns=["stock_code"])
    return sorted({str(code).zfill(6) for code in df["stock_code"].tolist() if supported_code(str(code).zfill(6))})


def parse_requested_codes(text: str) -> list[str]:
    parts = re.split(r"[\s,，;；]+", text.strip())
    codes: list[str] = []
    seen: set[str] = set()
    for part in parts:
        if not part:
            continue
        digits = re.sub(r"\D", "", part)
        if not digits:
            continue
        code = digits[-6:].zfill(6)
        if code in seen:
            continue
        seen.add(code)
        codes.append(code)
    return codes


def resolve_target_codes(selected: str | None = None, limit: int | None = None) -> list[str]:
    universe = load_supported_universe()
    if not selected:
        return universe[:limit] if limit is not None else universe
    requested = parse_requested_codes(selected)
    if not requested:
        raise ValueError("No valid stock codes were parsed from --codes.")
    invalid = [code for code in requested if not supported_code(code)]
    if invalid:
        raise ValueError(f"Unsupported stock codes in --codes: {', '.join(invalid)}")
    universe_set = set(universe)
    missing = [code for code in requested if code not in universe_set]
    if missing:
        raise ValueError(f"Stock codes not found in local universe: {', '.join(missing)}")
    return requested


def report_dates() -> list[str]:
    dates: list[str] = []
    today = dt.date.today()
    min_report_date = dt.date(1990, 12, 31)
    for year in range(1990, today.year + 1):
        for month, day in ((6, 30), (12, 31)):
            value = dt.date(year, month, day)
            if value < min_report_date or value > today:
                continue
            dates.append(value.strftime("%Y%m%d"))
    return dates


def call_with_retry(
    label: str,
    func,
    *args,
    retries: int = 3,
    delay: float = 1.0,
    non_retry_patterns: Sequence[str] = (),
    **kwargs,
):
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                return func(*args, **kwargs)
        except Exception as exc:
            last_error = exc
            text = repr(exc)
            if any(pattern in text for pattern in non_retry_patterns) or attempt == retries:
                break
            sleep_for = delay * attempt
            log(f"[retry] {label} attempt {attempt}/{retries} failed: {exc!r}; sleep {sleep_for:.1f}s")
            time.sleep(sleep_for)
    assert last_error is not None
    raise last_error


def normalize_bulk_dividend_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["code", "ex_date", "bonus", "transfer", "cash"])
    frame = df.copy()
    if "代码" not in frame.columns or "除权除息日" not in frame.columns:
        return pd.DataFrame(columns=["code", "ex_date", "bonus", "transfer", "cash"])
    frame["code"] = frame["代码"].astype(str).str.zfill(6)
    frame = frame[frame["code"].map(supported_code)]
    frame["status"] = frame["方案进度"].astype(str) if "方案进度" in frame.columns else ""
    frame = frame[frame["status"].str.contains("实施", na=False)]
    frame["ex_date"] = frame["除权除息日"].map(to_date_int)
    frame = frame[frame["ex_date"].notna()]
    frame = frame[frame["ex_date"] <= 今天]

    total_col = "送转股份-送转总比例" if "送转股份-送转总比例" in frame.columns else None
    bonus_col = "送转股份-送股比例" if "送转股份-送股比例" in frame.columns else None
    if bonus_col is None and "送转股份-送转比例" in frame.columns:
        bonus_col = "送转股份-送转比例"
    transfer_col = "送转股份-转股比例" if "送转股份-转股比例" in frame.columns else None
    cash_col = "现金分红-现金分红比例" if "现金分红-现金分红比例" in frame.columns else None

    if total_col and transfer_col:
        total_values = frame[total_col].map(safe_float)
        transfer_values = frame[transfer_col].map(safe_float)
        bonus_values = (total_values - transfer_values).where((total_values - transfer_values) > 0, 0.0)
    elif bonus_col:
        bonus_values = frame[bonus_col].map(safe_float)
    else:
        bonus_values = 0.0

    normalized = pd.DataFrame(
        {
            "code": frame["code"],
            "ex_date": frame["ex_date"].astype(int),
            "bonus": bonus_values,
            "transfer": frame[transfer_col].map(safe_float) if transfer_col else 0.0,
            "cash": frame[cash_col].map(safe_float) if cash_col else 0.0,
        }
    )
    return normalized[(normalized["bonus"] != 0) | (normalized["transfer"] != 0) | (normalized["cash"] != 0)]


def fetch_bulk_dividend_events(target_codes: Sequence[str] | None = None) -> tuple[dict[str, list[CorporateAction]], set[str]]:
    frames: list[pd.DataFrame] = []
    target_code_set = set(target_codes or [])
    for index, date_str in enumerate(report_dates(), start=1):
        try:
            df = call_with_retry(
                f"stock_fhps_em({date_str})",
                ak.stock_fhps_em,
                date=date_str,
                retries=2,
                non_retry_patterns=("NoneType",),
            )
        except Exception as exc:
            log(f"[warn] skip report {date_str}: {exc!r}")
            continue
        normalized = normalize_bulk_dividend_frame(df)
        if target_code_set:
            normalized = normalized[normalized["code"].isin(target_code_set)]
        if not normalized.empty:
            frames.append(normalized)
        if index % 20 == 0:
            log(f"[dividend-bulk] processed {index} report dates")

    merged: dict[str, dict[int, CorporateAction]] = defaultdict(dict)
    if frames:
        frame = pd.concat(frames, ignore_index=True)
        grouped = frame.groupby(["code", "ex_date"], as_index=False)[["bonus", "transfer", "cash"]].sum()
        for row in grouped.itertuples(index=False):
            merged[row.code][int(row.ex_date)] = CorporateAction(
                ex_date=int(row.ex_date),
                bonus_per10=safe_float(row.bonus),
                transfer_per10=safe_float(row.transfer),
                cash_div_per10=safe_float(row.cash),
            )
    result = {code: sorted(actions.values(), key=lambda item: item.ex_date, reverse=True) for code, actions in merged.items()}
    return result, set(result.keys())


def fetch_cninfo_dividends_for_code(code: str) -> list[CorporateAction]:
    try:
        df = call_with_retry(
            f"stock_dividend_cninfo({code})",
            ak.stock_dividend_cninfo,
            symbol=code,
            non_retry_patterns=("实施方案公告日期", "JSONDecodeError"),
        )
    except Exception as exc:
        if "实施方案公告日期" in repr(exc) or "JSONDecodeError" in repr(exc):
            return []
        raise
    if df.empty or "除权日" not in df.columns:
        return []
    rows = df.copy()
    rows["ex_date"] = rows["除权日"].map(to_date_int)
    rows = rows[rows["ex_date"].notna()]
    rows = rows[rows["ex_date"] <= 今天]
    grouped = defaultdict(lambda: {"bonus": 0.0, "transfer": 0.0, "cash": 0.0})
    for _, row in rows.iterrows():
        ex_date = int(row["ex_date"])
        grouped[ex_date]["bonus"] += safe_float(row.get("送股比例", 0.0))
        grouped[ex_date]["transfer"] += safe_float(row.get("转增比例", 0.0))
        grouped[ex_date]["cash"] += safe_float(row.get("派息比例", 0.0))
    return [
        CorporateAction(ex_date=ex_date, bonus_per10=values["bonus"], transfer_per10=values["transfer"], cash_div_per10=values["cash"])
        for ex_date, values in sorted(grouped.items(), reverse=True)
        if values["bonus"] or values["transfer"] or values["cash"]
    ]


def fetch_ths_dividends_for_code(code: str) -> list[CorporateAction]:
    try:
        df = call_with_retry(
            f"stock_fhps_detail_ths({code})",
            ak.stock_fhps_detail_ths,
            symbol=code,
            non_retry_patterns=("No tables found",),
        )
    except Exception as exc:
        if "No tables found" in repr(exc):
            return []
        raise
    if df.empty:
        return []
    frame = df.copy()
    date_col = "A股除权除息日" if "A股除权除息日" in frame.columns else None
    plan_col = "分红方案说明" if "分红方案说明" in frame.columns else None
    if date_col is None or plan_col is None:
        return []
    frame["ex_date"] = frame[date_col].map(to_date_int)
    frame = frame[frame["ex_date"].notna()]
    frame = frame[frame["ex_date"] <= 今天]
    grouped = defaultdict(lambda: {"bonus": 0.0, "transfer": 0.0, "cash": 0.0})
    for _, row in frame.iterrows():
        bonus, transfer, cash = parse_plan_text(str(row.get(plan_col, "")))
        if not bonus and not transfer and not cash:
            continue
        ex_date = int(row["ex_date"])
        grouped[ex_date]["bonus"] += bonus
        grouped[ex_date]["transfer"] += transfer
        grouped[ex_date]["cash"] += cash
    return [
        CorporateAction(ex_date=ex_date, bonus_per10=values["bonus"], transfer_per10=values["transfer"], cash_div_per10=values["cash"])
        for ex_date, values in sorted(grouped.items(), reverse=True)
        if values["bonus"] or values["transfer"] or values["cash"]
    ]


def fetch_rights_for_code(code: str) -> list[CorporateAction]:
    try:
        df = call_with_retry(
            f"stock_history_dividend_detail({code}, 配股)",
            ak.stock_history_dividend_detail,
            symbol=code,
            indicator="配股",
            non_retry_patterns=("No tables found",),
        )
    except Exception as exc:
        if "No tables found" in repr(exc):
            return []
        raise
    if df.empty or "除权日" not in df.columns:
        return []
    frame = df.copy()
    frame["ex_date"] = frame["除权日"].map(to_date_int)
    frame = frame[frame["ex_date"].notna()]
    frame = frame[frame["ex_date"] <= 今天]
    grouped = defaultdict(lambda: {"rights": 0.0, "price": 0.0})
    for _, row in frame.iterrows():
        rights_per10 = safe_float(row.get("配股方案", 0.0))
        rights_price = safe_float(row.get("配股价格", 0.0))
        if rights_per10 == 0.0:
            continue
        ex_date = int(row["ex_date"])
        grouped[ex_date]["rights"] += rights_per10
        grouped[ex_date]["price"] = rights_price
    return [
        CorporateAction(ex_date=ex_date, rights_per10=values["rights"], rights_price=values["price"])
        for ex_date, values in sorted(grouped.items(), reverse=True)
        if values["rights"]
    ]


def fetch_share_changes_for_code(code: str) -> list[ShareChange]:
    return fetch_share_changes_from_em_for_code(code)


def finalize_share_changes(shares: Sequence[tuple[int, int]]) -> list[ShareChange]:
    if not shares:
        return []
    dedup: dict[int, int] = {}
    for change_date, shares_int in sorted(shares):
        dedup[change_date] = shares_int
    result: list[ShareChange] = []
    previous = None
    for change_date, shares_int in sorted(dedup.items()):
        if previous == shares_int:
            continue
        result.append(ShareChange(change_date=change_date, float_shares=shares_int))
        previous = shares_int
    return sorted(result, key=lambda item: item.change_date, reverse=True)


def fetch_share_changes_from_cninfo_for_code(code: str) -> list[ShareChange]:
    try:
        df = call_with_retry(
            f"stock_share_change_cninfo({code})",
            ak.stock_share_change_cninfo,
            symbol=code,
            start_date="19900101",
            end_date=str(今天),
            non_retry_patterns=("公告日期", "JSONDecodeError"),
        )
    except Exception as exc:
        text = repr(exc)
        if "公告日期" in text or "JSONDecodeError" in text:
            return []
        raise
    if df.empty or "变动日期" not in df.columns:
        return []
    frame = df.copy()
    frame["change_date"] = frame["变动日期"].map(to_date_int)
    frame = frame[frame["change_date"].notna()]
    frame = frame[frame["change_date"] <= 今天]
    shares: list[tuple[int, int]] = []
    for _, row in frame.iterrows():
        value = safe_float(row.get("已流通股份", 0.0))
        if value == 0.0:
            value = safe_float(row.get("人民币普通股", 0.0))
        if value == 0.0:
            continue
        shares_int = int(round(value * 10_000))
        if shares_int > 0:
            shares.append((int(row["change_date"]), shares_int))
    return finalize_share_changes(shares)


def fetch_share_changes_from_em_for_code(code: str) -> list[ShareChange]:
    url = "https://datacenter.eastmoney.com/securities/api/data/v1/get"
    symbol = eastmoney_symbol(code)
    columns = (
        "SECUCODE,SECURITY_CODE,END_DATE,TOTAL_SHARES,UNLIMITED_SHARES,LISTED_A_SHARES,"
        "B_FREE_SHARE,H_FREE_SHARE,LIMITED_SHARES,LIMITED_A_SHARES,NON_FREE_SHARES,LOCK_SHARES,CHANGE_REASON"
    )
    shares: list[tuple[int, int]] = []
    page_number = 1
    total_pages = 1
    while page_number <= total_pages:
        params = {
            "reportName": "RPT_F10_EH_EQUITY",
            "columns": columns,
            "quoteColumns": "",
            "filter": f'(SECUCODE="{symbol}")',
            "pageNumber": str(page_number),
            "pageSize": "200",
            "sortTypes": "-1",
            "sortColumns": "END_DATE",
            "source": "HSF10",
            "client": "PC",
        }
        response = call_with_retry(
            f"eastmoney_share_change({code}, page={page_number})",
            requests.get,
            url,
            params=params,
            timeout=20,
        )
        response.raise_for_status()
        data_json = response.json()
        result = data_json.get("result") or {}
        rows = result.get("data") or []
        total_pages = int(result.get("pages") or 1)
        for row in rows:
            change_date = to_date_int(row.get("END_DATE"))
            if change_date is None or change_date > 今天:
                continue
            value = safe_float(row.get("LISTED_A_SHARES"))
            if value == 0.0:
                unlimited_shares = safe_float(row.get("UNLIMITED_SHARES"))
                b_free = safe_float(row.get("B_FREE_SHARE"))
                h_free = safe_float(row.get("H_FREE_SHARE"))
                if unlimited_shares > 0.0:
                    value = max(unlimited_shares - b_free - h_free, 0.0)
            if value == 0.0:
                value = safe_float(row.get("UNLIMITED_SHARES"))
            if value == 0.0:
                total_shares = safe_float(row.get("TOTAL_SHARES"))
                limited_a = safe_float(row.get("LIMITED_A_SHARES"))
                limited_all = safe_float(row.get("LIMITED_SHARES"))
                limited_value = limited_a or limited_all
                if total_shares > 0.0 and limited_value >= 0.0:
                    value = max(total_shares - limited_value, 0.0)
            shares_int = int(round(value))
            if shares_int > 0:
                shares.append((int(change_date), shares_int))
        page_number += 1
    return finalize_share_changes(shares)


def build_local_share_change_fallback(codes: Sequence[str]) -> dict[str, list[ShareChange]]:
    if not codes:
        return {}
    target_codes = set(codes)
    frame = pd.read_parquet(日线数据路径, columns=["stock_code", "date", "outstanding_share"])
    frame["stock_code"] = frame["stock_code"].astype(str).str.zfill(6)
    frame = frame[frame["stock_code"].isin(target_codes)].copy()
    if frame.empty:
        return {}
    frame["date_key"] = pd.to_datetime(frame["date"]).dt.strftime("%Y%m%d").astype(int)
    results: dict[str, list[ShareChange]] = {}
    for code, group in frame.groupby("stock_code", sort=False):
        series = group.sort_values("date_key")[["date_key", "outstanding_share"]].copy()
        series["outstanding_share"] = series["outstanding_share"].map(safe_float)
        series = series[series["outstanding_share"] > 0]
        if series.empty:
            continue
        smoothed = series["outstanding_share"].rolling(window=5, min_periods=1, center=True).median()
        median_share = float(smoothed.median())
        granularity = max(100_000, int(median_share * 0.001)) or 100_000
        rounded_share = ((smoothed / granularity).round().astype("int64") * granularity).tolist()
        for idx in range(1, len(rounded_share) - 1):
            prev_value = rounded_share[idx - 1]
            curr_value = rounded_share[idx]
            next_value = rounded_share[idx + 1]
            if prev_value <= 0 or next_value <= 0:
                continue
            neighbor_ratio = abs(prev_value - next_value) / max(prev_value, 1)
            curr_prev_ratio = abs(curr_value - prev_value) / max(prev_value, 1)
            curr_next_ratio = abs(curr_value - next_value) / max(next_value, 1)
            if neighbor_ratio <= 0.01 and curr_prev_ratio >= 0.2 and curr_next_ratio >= 0.2:
                rounded_share[idx] = prev_value
        series["rounded_share"] = rounded_share
        dedup = series.loc[series["rounded_share"].ne(series["rounded_share"].shift()), ["date_key", "rounded_share"]]
        if dedup.empty:
            continue
        results[code] = [
            ShareChange(change_date=int(row.date_key), float_shares=int(row.rounded_share))
            for row in dedup.iloc[::-1].itertuples(index=False)
        ]
    return results


def merge_actions(
    codes: Sequence[str],
    bulk_dividends: dict[str, list[CorporateAction]],
    cninfo_dividends: dict[str, list[CorporateAction]],
    ths_dividends: dict[str, list[CorporateAction]],
    rights_actions: dict[str, list[CorporateAction]],
) -> dict[str, list[CorporateAction]]:
    merged: dict[str, list[CorporateAction]] = {}
    for code in codes:
        by_date: dict[int, CorporateAction] = {}
        for source in (
            bulk_dividends.get(code, []),
            cninfo_dividends.get(code, []),
            ths_dividends.get(code, []),
            rights_actions.get(code, []),
        ):
            for action in source:
                existing = by_date.get(action.ex_date)
                if existing is None:
                    by_date[action.ex_date] = action
                else:
                    by_date[action.ex_date] = CorporateAction(
                        ex_date=action.ex_date,
                        bonus_per10=existing.bonus_per10 + action.bonus_per10,
                        transfer_per10=existing.transfer_per10 + action.transfer_per10,
                        cash_div_per10=existing.cash_div_per10 + action.cash_div_per10,
                        rights_per10=existing.rights_per10 + action.rights_per10,
                        rights_price=action.rights_price or existing.rights_price,
                    )
        merged[code] = sorted(by_date.values(), key=lambda item: item.ex_date, reverse=True)
    return merged


def run_parallel_map(
    codes: Sequence[str],
    fetcher,
    workers: int,
    label: str,
    backend: str = "process",
) -> dict[str, list]:
    if not codes:
        return {}
    results: dict[str, list] = {}
    completed = 0
    executor_cls = concurrent.futures.ProcessPoolExecutor if backend == "process" else concurrent.futures.ThreadPoolExecutor
    with executor_cls(max_workers=workers) as executor:
        future_map = {executor.submit(fetcher, code): code for code in codes}
        for future in concurrent.futures.as_completed(future_map):
            code = future_map[future]
            try:
                results[code] = future.result()
            except Exception as exc:
                log(f"[warn] {label} {code} failed: {exc!r}")
                results[code] = []
            completed += 1
            if completed % 100 == 0 or completed == len(codes):
                log(f"[{label}] {completed}/{len(codes)}")
    return results
