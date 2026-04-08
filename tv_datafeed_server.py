"""Local TradingView datafeed server backed by the latest parquet files.

The server exposes a small UDF-style HTTP API so TradingView Desktop can
request bars directly from the local parquet snapshots instead of relying on
its own cached data.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import re
from dataclasses import dataclass
from functools import lru_cache
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse
from zoneinfo import ZoneInfo

import pandas as pd
import pyarrow.parquet as pq


PROJECT_DIR = Path(__file__).resolve().parent
DATA_DIR = PROJECT_DIR / "数据"
TZ = ZoneInfo("Asia/Shanghai")

STOCK_DAILY = DATA_DIR / "全市场历史数据.parquet"
STOCK_M15 = DATA_DIR / "全市场15分钟数据.parquet"
STOCK_M30 = DATA_DIR / "全市场30分钟数据.parquet"
STOCK_M60 = DATA_DIR / "全市场60分钟数据.parquet"
STOCK_MONTHLY = DATA_DIR / "全市场月线数据.parquet"

INDEX_DAILY = DATA_DIR / "上证指数日线数据.parquet"
INDEX_M15 = DATA_DIR / "上证指数15分钟数据.parquet"
INDEX_M30 = DATA_DIR / "上证指数30分钟数据.parquet"
INDEX_M60 = DATA_DIR / "上证指数60分钟数据.parquet"
INDEX_MONTHLY = DATA_DIR / "上证指数月线数据.parquet"


@dataclass(frozen=True)
class SymbolRecord:
    symbol: str
    code: str
    name: str
    kind: str  # "stock" or "index"
    exchange: str
    source: str  # "stock" or "index"


def _log(msg: str) -> None:
    now = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now}] {msg}", flush=True)


def _normalize_code(value: str | Any) -> str:
    text = "" if value is None else str(value).strip()
    match = re.search(r"(\d{6})", text)
    return match.group(1) if match else ""


def _normalize_name(value: str | Any) -> str:
    return "" if value is None else str(value).strip()


def _normalize_symbol_hint(value: str | Any) -> str:
    text = "" if value is None else str(value).strip().upper()
    text = text.replace("/", ".")
    return text


def _latest_date_from_parquet(path: Path, date_col: str = "date") -> str | None:
    if not path.exists():
        return None

    try:
        pf = pq.ParquetFile(path)
    except Exception:
        return None

    names = pf.metadata.schema.names
    if date_col not in names:
        return None

    idx = names.index(date_col)
    max_date: str | None = None
    saw_stats = False

    for i in range(pf.metadata.num_row_groups):
        stats = pf.metadata.row_group(i).column(idx).statistics
        if stats is None or stats.max is None:
            continue
        saw_stats = True
        candidate = _normalize_date_value(stats.max)
        if candidate and (max_date is None or candidate > max_date):
            max_date = candidate

    if max_date is not None or saw_stats:
        return max_date

    for batch in pf.iter_batches(columns=[date_col], batch_size=500_000):
        col = batch.column(0)
        for item in col.to_pylist():
            candidate = _normalize_date_value(item)
            if candidate and (max_date is None or candidate > max_date):
                max_date = candidate
    return max_date


def _normalize_date_value(value: Any) -> str | None:
    if value is None:
        return None
    if hasattr(value, "as_py"):
        value = value.as_py()
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="ignore")
    if hasattr(value, "to_pydatetime"):
        value = value.to_pydatetime()
    if isinstance(value, dt.datetime):
        return value.date().isoformat()
    if isinstance(value, dt.date):
        return value.isoformat()
    text = str(value).strip()
    if len(text) >= 10 and text[4:5] == "-" and text[7:8] == "-":
        return text[:10]
    return None


def _normalize_time_value(value: Any) -> str:
    text = "" if value is None else str(value).strip()
    if re.fullmatch(r"\d{14,17}", text):
        return f"{text[8:10]}:{text[10:12]}:{text[12:14]}"
    if re.fullmatch(r"\d{2}:\d{2}", text):
        return f"{text}:00"
    if re.fullmatch(r"\d{2}:\d{2}:\d{2}", text):
        return text
    return ""


def _epoch_from_date(date_str: str) -> int:
    return int(dt.datetime.fromisoformat(date_str).replace(tzinfo=TZ).timestamp())


def _epoch_from_datetime(date_str: str, time_str: str) -> int:
    return int(dt.datetime.fromisoformat(f"{date_str} {time_str}").replace(tzinfo=TZ).timestamp())


def _to_float_columns(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    out = df.copy()
    for col in cols:
        out[col] = pd.to_numeric(out[col], errors="coerce")
    return out


def _stock_exchange(code: str) -> str:
    return "SSE" if code.startswith(("6", "9")) else "SZSE"


def _index_symbol() -> str:
    return "SSE:000001"


class MarketDataStore:
    def __init__(self) -> None:
        self.stock_latest_date = _latest_date_from_parquet(STOCK_DAILY)
        self.index_latest_date = _latest_date_from_parquet(INDEX_DAILY)
        self.stock_current = self._load_current_stock_registry()
        self.index_current = self._load_current_index_registry()

    def _load_current_stock_registry(self) -> dict[str, str]:
        if not STOCK_DAILY.exists() or not self.stock_latest_date:
            return {}
        try:
            df = pd.read_parquet(
                STOCK_DAILY,
                columns=["stock_code", "stock_name", "date"],
                filters=[("date", "=", self.stock_latest_date)],
            )
        except Exception:
            return {}

        if df.empty:
            return {}

        df["stock_code"] = df["stock_code"].astype(str).str.extract(r"(\d{6})", expand=False).fillna("")
        df["stock_name"] = df["stock_name"].fillna("").astype(str).str.strip()
        df = df[(df["stock_code"] != "") & (df["stock_name"] != "")]
        if df.empty:
            return {}
        dedup = df.drop_duplicates(subset=["stock_code"], keep="last")
        return dict(zip(dedup["stock_code"], dedup["stock_name"]))

    def _load_current_index_registry(self) -> dict[str, str]:
        if not INDEX_DAILY.exists() or not self.index_latest_date:
            return {}
        try:
            df = pd.read_parquet(
                INDEX_DAILY,
                columns=["index_code", "index_name", "date"],
                filters=[("date", "=", self.index_latest_date)],
            )
        except Exception:
            return {}

        if df.empty:
            return {}

        df["index_code"] = df["index_code"].astype(str).str.extract(r"(\d{6})", expand=False).fillna("")
        df["index_name"] = df["index_name"].fillna("").astype(str).str.strip()
        df = df[(df["index_code"] != "") & (df["index_name"] != "")]
        if df.empty:
            return {}
        dedup = df.drop_duplicates(subset=["index_code"], keep="last")
        return dict(zip(dedup["index_code"], dedup["index_name"]))

    @lru_cache(maxsize=2048)
    def resolve(self, token: str) -> SymbolRecord | None:
        hint = _normalize_symbol_hint(token)
        code = _normalize_code(hint)
        if not code:
            if hint == "上证指数":
                code = "000001"
                return SymbolRecord(symbol=_index_symbol(), code=code, name="上证指数", kind="index", exchange="SSE", source="index")
            return None

        # Explicit index aliases.
        if hint in {"SSE:000001", "SH.000001", "SHSE:000001", "000001.SH", "000001.SSE", "上证指数"}:
            name = self._resolve_index_name(code) or "上证指数"
            return SymbolRecord(symbol=_index_symbol(), code=code, name=name, kind="index", exchange="SSE", source="index")

        # Current stock universe first.
        if code in self.stock_current:
            name = self.stock_current[code]
            return SymbolRecord(symbol=f"{_stock_exchange(code)}:{code}", code=code, name=name, kind="stock", exchange=_stock_exchange(code), source="stock")

        # Historical stock fallback.
        name = self._resolve_historical_stock_name(code)
        if name:
            return SymbolRecord(symbol=f"{_stock_exchange(code)}:{code}", code=code, name=name, kind="stock", exchange=_stock_exchange(code), source="stock")

        # Historical index fallback for explicit prefixed forms.
        if hint.startswith(("SSE:", "SH.", "SHSE:")):
            name = self._resolve_index_name(code)
            if name:
                return SymbolRecord(symbol=_index_symbol(), code=code, name=name, kind="index", exchange="SSE", source="index")
        return None

    @lru_cache(maxsize=2048)
    def _resolve_historical_stock_name(self, code: str) -> str | None:
        if not STOCK_DAILY.exists():
            return None
        try:
            df = pd.read_parquet(STOCK_DAILY, columns=["stock_code", "stock_name", "date"], filters=[("stock_code", "=", code)])
        except Exception:
            return None
        if df.empty:
            return None
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df[df["date"].notna()].copy()
        if df.empty:
            return None
        row = df.sort_values("date", kind="mergesort").iloc[-1]
        name = _normalize_name(row.get("stock_name"))
        return name or None

    @lru_cache(maxsize=2048)
    def _resolve_index_name(self, code: str) -> str | None:
        if not INDEX_DAILY.exists():
            return None
        try:
            df = pd.read_parquet(INDEX_DAILY, columns=["index_code", "index_name", "date"], filters=[("index_code", "=", f"sh.{code}")])
        except Exception:
            return None
        if df.empty:
            return self.index_current.get(code)
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df[df["date"].notna()].copy()
        if df.empty:
            return self.index_current.get(code)
        row = df.sort_values("date", kind="mergesort").iloc[-1]
        name = _normalize_name(row.get("index_name"))
        return name or self.index_current.get(code)

    def search(self, query: str, limit: int = 50) -> list[dict[str, Any]]:
        q = _normalize_symbol_hint(query)
        if not q:
            # Return a small useful default list.
            return self._top_symbols(limit)

        results: list[dict[str, Any]] = []

        direct = self.resolve(q)
        if direct:
            results.append(self._record_to_search_result(direct))

        q_code = _normalize_code(q)
        if q_code:
            # Include stock and index candidates even if one exact result already exists.
            if q_code in self.stock_current:
                results.append(
                    self._record_to_search_result(
                        SymbolRecord(
                            symbol=f"{_stock_exchange(q_code)}:{q_code}",
                            code=q_code,
                            name=self.stock_current[q_code],
                            kind="stock",
                            exchange=_stock_exchange(q_code),
                            source="stock",
                        )
                    )
                )
            index_name = self.index_current.get(q_code) or self._resolve_index_name(q_code)
            if index_name:
                results.append(
                    self._record_to_search_result(
                        SymbolRecord(
                            symbol=_index_symbol(),
                            code=q_code,
                            name=index_name,
                            kind="index",
                            exchange="SSE",
                            source="index",
                        )
                    )
                )

        # Name search over current registry and the index name.
        if not results:
            lower_q = q.lower()
            for code, name in self.stock_current.items():
                if lower_q in name.lower() or lower_q in code:
                    results.append(
                        self._record_to_search_result(
                            SymbolRecord(
                                symbol=f"{_stock_exchange(code)}:{code}",
                                code=code,
                                name=name,
                                kind="stock",
                                exchange=_stock_exchange(code),
                                source="stock",
                            )
                        )
                    )
                    if len(results) >= limit:
                        return results[:limit]

            for code, name in self.index_current.items():
                if lower_q in name.lower() or lower_q in code:
                    results.append(
                        self._record_to_search_result(
                            SymbolRecord(
                                symbol=_index_symbol(),
                                code=code,
                                name=name,
                                kind="index",
                                exchange="SSE",
                                source="index",
                            )
                        )
                    )
                    if len(results) >= limit:
                        return results[:limit]

        deduped: list[dict[str, Any]] = []
        seen: set[str] = set()
        for item in results:
            sym = str(item.get("symbol", ""))
            if sym and sym not in seen:
                deduped.append(item)
                seen.add(sym)
            if len(deduped) >= limit:
                break
        return deduped

    def _top_symbols(self, limit: int) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        for code in sorted(self.stock_current.keys())[:limit]:
            results.append(
                self._record_to_search_result(
                    SymbolRecord(
                        symbol=f"{_stock_exchange(code)}:{code}",
                        code=code,
                        name=self.stock_current[code],
                        kind="stock",
                        exchange=_stock_exchange(code),
                        source="stock",
                    )
                )
            )
        if len(results) < limit:
            for code in sorted(self.index_current.keys()):
                results.append(
                    self._record_to_search_result(
                        SymbolRecord(
                            symbol=_index_symbol(),
                            code=code,
                            name=self.index_current[code],
                            kind="index",
                            exchange="SSE",
                            source="index",
                        )
                    )
                )
                if len(results) >= limit:
                    break
        return results[:limit]

    def _record_to_search_result(self, record: SymbolRecord) -> dict[str, Any]:
        return {
            "symbol": record.symbol,
            "full_name": record.name,
            "description": record.name,
            "exchange": record.exchange,
            "ticker": record.code,
            "type": record.kind,
        }

    def _select_path(self, record: SymbolRecord, resolution: str) -> tuple[Path, str, str]:
        res = resolution.upper()
        if record.kind == "index":
            if res in {"15", "15M"}:
                return INDEX_M15, "index_code", "index_name"
            if res in {"30", "30M"}:
                return INDEX_M30, "index_code", "index_name"
            if res in {"60", "1H", "60M"}:
                return INDEX_M60, "index_code", "index_name"
            if res in {"D", "1D"}:
                return INDEX_DAILY, "index_code", "index_name"
            if res in {"W", "1W"}:
                return INDEX_DAILY, "index_code", "index_name"
            if res in {"M", "1M"}:
                return INDEX_MONTHLY, "index_code", "index_name"
            raise ValueError(f"Unsupported resolution for index: {resolution}")

        if res in {"15", "15M"}:
            return STOCK_M15, "stock_code", "stock_name"
        if res in {"30", "30M"}:
            return STOCK_M30, "stock_code", "stock_name"
        if res in {"60", "1H", "60M"}:
            return STOCK_M60, "stock_code", "stock_name"
        if res in {"D", "1D"}:
            return STOCK_DAILY, "stock_code", "stock_name"
        if res in {"W", "1W"}:
            return STOCK_DAILY, "stock_code", "stock_name"
        if res in {"M", "1M"}:
            return STOCK_MONTHLY, "stock_code", "stock_name"
        raise ValueError(f"Unsupported resolution: {resolution}")

    def history(
        self,
        symbol_token: str,
        resolution: str,
        start_ts: int | None,
        end_ts: int | None,
        countback: int | None = None,
    ) -> dict[str, Any]:
        record = self.resolve(symbol_token)
        if record is None:
            return {"s": "no_data"}

        try:
            path, code_col, name_col = self._select_path(record, resolution)
        except ValueError:
            return {"s": "no_data"}

        if not path.exists():
            return {"s": "no_data"}

        if record.kind == "index":
            code_value = f"sh.{record.code}"
        else:
            code_value = record.code

        cols = [code_col, name_col, "date", "open", "high", "low", "close", "volume", "amount"]
        if resolution.upper() in {"15", "15M", "30", "30M", "60", "1H", "60M"}:
            cols.insert(3, "time")

        filters: list[tuple[str, str, Any]] = [(code_col, "=", code_value)]
        if start_ts is not None:
            filters.append(("date", ">=", self._date_from_ts(start_ts)))
        if end_ts is not None:
            filters.append(("date", "<=", self._date_from_ts(end_ts)))

        try:
            df = pd.read_parquet(path, columns=cols, filters=filters)
        except Exception:
            return {"s": "no_data"}

        if df.empty:
            return {"s": "no_data"}

        if resolution.upper() in {"W", "1W"}:
            df = self._aggregate_weekly(df, code_col, name_col)
        elif resolution.upper() in {"M", "1M"} and path != (STOCK_MONTHLY if record.kind == "stock" else INDEX_MONTHLY):
            df = self._aggregate_monthly(df, code_col, name_col)

        if countback is not None and countback > 0 and len(df) > countback:
            df = df.tail(countback).copy()

        df = self._normalize_history_frame(df, record.kind, resolution)
        if df.empty:
            return {"s": "no_data"}

        return {
            "s": "ok",
            "t": df["timestamp"].tolist(),
            "o": df["open"].tolist(),
            "h": df["high"].tolist(),
            "l": df["low"].tolist(),
            "c": df["close"].tolist(),
            "v": df["volume"].tolist(),
        }

    def _date_from_ts(self, value: int) -> str:
        return dt.datetime.fromtimestamp(value, tz=dt.timezone.utc).astimezone(TZ).date().isoformat()

    def _aggregate_weekly(self, df: pd.DataFrame, code_col: str, name_col: str) -> pd.DataFrame:
        out = df.copy()
        out["date"] = pd.to_datetime(out["date"], errors="coerce")
        out = out[out["date"].notna()].copy()
        if out.empty:
            return out
        out = _to_float_columns(out, ["open", "high", "low", "close", "volume", "amount"])
        out["week"] = out["date"].dt.to_period("W-FRI")
        weekly = (
            out.sort_values(["date"], kind="mergesort")
            .groupby("week", as_index=False)
            .agg(
                {
                    code_col: "last",
                    name_col: "last",
                    "date": "max",
                    "open": "first",
                    "high": "max",
                    "low": "min",
                    "close": "last",
                    "volume": "sum",
                    "amount": "sum",
                }
            )
        )
        return weekly

    def _aggregate_monthly(self, df: pd.DataFrame, code_col: str, name_col: str) -> pd.DataFrame:
        out = df.copy()
        out["date"] = pd.to_datetime(out["date"], errors="coerce")
        out = out[out["date"].notna()].copy()
        if out.empty:
            return out
        out = _to_float_columns(out, ["open", "high", "low", "close", "volume", "amount"])
        out["month"] = out["date"].dt.to_period("M")
        monthly = (
            out.sort_values(["date"], kind="mergesort")
            .groupby("month", as_index=False)
            .agg(
                {
                    code_col: "last",
                    name_col: "last",
                    "date": "max",
                    "open": "first",
                    "high": "max",
                    "low": "min",
                    "close": "last",
                    "volume": "sum",
                    "amount": "sum",
                }
            )
        )
        return monthly

    def _normalize_history_frame(self, df: pd.DataFrame, kind: str, resolution: str) -> pd.DataFrame:
        out = df.copy()
        out["date"] = pd.to_datetime(out["date"], errors="coerce")
        out = out[out["date"].notna()].copy()
        if out.empty:
            return out

        out = _to_float_columns(out, ["open", "high", "low", "close", "volume", "amount"])

        res = resolution.upper()
        if res in {"15", "15M", "30", "30M", "60", "1H", "60M"}:
            out["time"] = out.get("time", "").map(_normalize_time_value)
            out = out[out["time"] != ""].copy()
            out["timestamp"] = [
                _epoch_from_datetime(d.strftime("%Y-%m-%d"), t)
                for d, t in zip(out["date"], out["time"], strict=False)
            ]
        else:
            out["timestamp"] = [ _epoch_from_date(d.strftime("%Y-%m-%d")) for d in out["date"] ]

        out = out.sort_values("timestamp", kind="mergesort").drop_duplicates(subset=["timestamp"], keep="last").reset_index(drop=True)
        out["open"] = out["open"].fillna(0.0)
        out["high"] = out["high"].fillna(out["open"])
        out["low"] = out["low"].fillna(out["open"])
        out["close"] = out["close"].fillna(out["open"])
        out["volume"] = out["volume"].fillna(0.0)

        return out[["timestamp", "open", "high", "low", "close", "volume"]]

    def symbol_info(self, token: str) -> dict[str, Any] | None:
        record = self.resolve(token)
        if record is None:
            return None

        if record.kind == "index":
            supports = ["D", "W", "M", "15", "30", "60"]
            session = "0930-1130,1300-1500"
        else:
            supports = ["D", "W", "M", "15", "30", "60"]
            session = "0930-1130,1300-1500"

        return {
            "name": record.symbol,
            "ticker": record.symbol,
            "description": record.name,
            "type": record.kind,
            "exchange": record.exchange,
            "listed_exchange": record.exchange,
            "timezone": "Asia/Shanghai",
            "session": session,
            "has_intraday": True,
            "has_daily": True,
            "has_weekly_and_monthly": True,
            "supported_resolutions": supports,
            "intraday_multipliers": ["15", "30", "60"],
            "daily_multipliers": ["1"],
            "pricescale": 10000,
            "minmov": 1,
            "volume_precision": 0,
            "data_status": "streaming",
            "has_empty_bars": False,
            "has_no_volume": False,
        }


STORE = MarketDataStore()


class TradingViewHandler(BaseHTTPRequestHandler):
    server_version = "TVParquetDatafeed/1.0"

    def _send_json(self, payload: Any, status: int = 200) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self) -> None:  # noqa: N802
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"
        params = {k: v[0] for k, v in parse_qs(parsed.query).items()}

        if path == "/":
            self._send_json(
                {
                    "status": "ok",
                    "service": self.server_version,
                    "stock_daily": str(STOCK_DAILY),
                    "index_daily": str(INDEX_DAILY),
                    "stock_latest": STORE.stock_latest_date,
                    "index_latest": STORE.index_latest_date,
                }
            )
            return

        if path == "/ping" or path == "/time":
            self._send_json({"s": "ok", "time": int(dt.datetime.now(tz=dt.timezone.utc).timestamp())})
            return

        if path == "/config":
            self._send_json(
                {
                    "supports_search": True,
                    "supports_group_request": False,
                    "supports_marks": False,
                    "supports_timescale_marks": False,
                    "supports_time": True,
                    "exchanges": [
                        {"value": "SSE", "name": "Shanghai", "desc": "Shanghai Stock Exchange"},
                        {"value": "SZSE", "name": "Shenzhen", "desc": "Shenzhen Stock Exchange"},
                    ],
                    "symbols_types": [
                        {"name": "Stock", "value": "stock"},
                        {"name": "Index", "value": "index"},
                    ],
                }
            )
            return

        if path == "/search":
            query = params.get("query", "")
            limit = int(params.get("limit", "50") or "50")
            self._send_json(STORE.search(query, limit=limit))
            return

        if path in {"/symbol", "/symbols", "/resolve"}:
            token = params.get("symbol") or params.get("query") or params.get("name") or ""
            info = STORE.symbol_info(token)
            if info is None:
                self._send_json({"s": "no_data"}, status=404)
            else:
                self._send_json(info)
            return

        if path == "/history":
            symbol = params.get("symbol", "")
            resolution = params.get("resolution", "D")
            start = params.get("from")
            end = params.get("to")
            countback = params.get("countback")
            start_ts = int(start) if start and start.isdigit() else None
            end_ts = int(end) if end and end.isdigit() else None
            countback_i = int(countback) if countback and countback.isdigit() else None
            self._send_json(STORE.history(symbol, resolution, start_ts, end_ts, countback_i))
            return

        self._send_json({"error": "not found"}, status=404)

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
        _log(format % args)


def main() -> None:
    parser = argparse.ArgumentParser(description="TradingView parquet datafeed server")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8001)
    args = parser.parse_args()

    _log("Loading symbol registry...")
    _log(f"  stock latest: {STORE.stock_latest_date or '-'}")
    _log(f"  index latest: {STORE.index_latest_date or '-'}")
    _log(f"  current stocks: {len(STORE.stock_current):,}")
    _log(f"  current index symbols: {len(STORE.index_current):,}")

    server = ThreadingHTTPServer((args.host, args.port), TradingViewHandler)
    server.daemon_threads = True
    _log(f"TradingView parquet datafeed listening on http://{args.host}:{args.port}")
    _log("Supported routes: /config /search /symbols /history /ping /time")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        _log("Shutting down...")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
