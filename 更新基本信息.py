"""更新全市场股票基本信息快照（总市值、流通市值等）。"""

from __future__ import annotations

import argparse
import time
from pathlib import Path
from typing import Sequence

import pandas as pd
import requests


项目目录 = Path(__file__).resolve().parent
默认输出路径 = 项目目录 / "数据" / "股票基本信息.parquet"
日线路径 = 项目目录 / "数据" / "全市场历史数据.parquet"
腾讯接口 = "https://qt.gtimg.cn/q={symbols}"
请求头 = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://gu.qq.com/",
}
批大小 = 160
默认最小行数 = 4500


def log(msg: str) -> None:
    print(msg, flush=True)


def _代码转腾讯符号(stock_code: str) -> str:
    if stock_code.startswith(("6", "9")):
        return f"sh{stock_code}"
    if stock_code.startswith(("4", "8")):
        return f"bj{stock_code}"
    return f"sz{stock_code}"


def _读取股票宇宙() -> pd.DataFrame:
    frame = pd.read_parquet(日线路径, columns=["stock_code", "stock_name"])
    frame["stock_code"] = frame["stock_code"].astype(str).str.strip()
    frame = frame[frame["stock_code"].str.fullmatch(r"\d{6}", na=False)].copy()
    frame = (
        frame.sort_values(["stock_code"], kind="mergesort")
        .drop_duplicates(subset=["stock_code"], keep="last")
        .reset_index(drop=True)
    )
    frame["symbol"] = frame["stock_code"].map(_代码转腾讯符号)
    return frame


def _安全转浮点(value: str | None) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return float(text)
    except Exception:
        return None


def _解析成交字段(payload: str) -> tuple[float | None, float | None]:
    parts = str(payload or "").split("/")
    if len(parts) >= 3:
        volume_lot = _安全转浮点(parts[1])
        amount_yuan = _安全转浮点(parts[2])
        volume = volume_lot * 100.0 if volume_lot is not None else None
        return volume, amount_yuan
    return None, None


def _解析腾讯行情行(line: str, snapshot_date: str) -> dict[str, object] | None:
    line = line.strip().rstrip(";")
    if not line or "=\"" not in line:
        return None
    symbol = line.split("=", 1)[0].removeprefix("v_")
    payload = line.split('="', 1)[1].rsplit('"', 1)[0]
    parts = payload.split("~")
    if len(parts) < 74:
        return None

    volume, amount = _解析成交字段(parts[35] if len(parts) > 35 else "")
    close = _安全转浮点(parts[3])
    total_shares = _安全转浮点(parts[73] if len(parts) > 73 else None)
    float_shares = _安全转浮点(parts[72] if len(parts) > 72 else None)

    total_market_cap = close * total_shares if close is not None and total_shares is not None else None
    float_market_cap = close * float_shares if close is not None and float_shares is not None else None

    stock_code = parts[2].strip()
    if not stock_code or not stock_code.isdigit():
        if symbol.startswith(("sh", "sz", "bj")) and len(symbol) >= 8:
            stock_code = symbol[2:]
    if not stock_code or not stock_code.isdigit():
        return None

    return {
        "stock_code": stock_code,
        "stock_name": parts[1].strip(),
        "date": snapshot_date,
        "close": close,
        "change_pct": _安全转浮点(parts[32] if len(parts) > 32 else None),
        "change_amount": _安全转浮点(parts[31] if len(parts) > 31 else None),
        "volume": volume,
        "amount": amount,
        "amplitude": _安全转浮点(parts[43] if len(parts) > 43 else None),
        "high": _安全转浮点(parts[33] if len(parts) > 33 else None),
        "low": _安全转浮点(parts[34] if len(parts) > 34 else None),
        "open": _安全转浮点(parts[5] if len(parts) > 5 else None),
        "prev_close": _安全转浮点(parts[4] if len(parts) > 4 else None),
        "turnover": _安全转浮点(parts[38] if len(parts) > 38 else None),
        "total_market_cap": total_market_cap,
        "float_market_cap": float_market_cap,
        "total_shares": total_shares,
        "float_shares": float_shares,
    }


def _抓取腾讯批量行情(symbols: list[str]) -> list[dict[str, object]]:
    url = 腾讯接口.format(symbols=",".join(symbols))
    last_exc: Exception | None = None
    for attempt in range(4):
        try:
            response = requests.get(url, headers=请求头, timeout=20)
            response.raise_for_status()
            text = response.text
            if not text.strip():
                raise RuntimeError("empty response")
            return [line for line in text.split(";") if line.strip()]
        except Exception as exc:
            last_exc = exc
            time.sleep(1.0 * (attempt + 1))
    raise RuntimeError(f"腾讯行情抓取失败: {last_exc!r}")


def 整理基本信息快照(raw: pd.DataFrame, snapshot_date: str) -> pd.DataFrame:
    if raw.empty:
        return pd.DataFrame(
            columns=[
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
        )

    keep_cols = [
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
    out = raw.loc[:, keep_cols].copy()
    out["stock_code"] = out["stock_code"].astype(str).str.strip()
    out = out[out["stock_code"].str.fullmatch(r"\d{6}", na=False)].copy()
    out["stock_name"] = out["stock_name"].astype(str).str.strip()
    out["date"] = snapshot_date

    numeric_cols = [col for col in keep_cols if col not in {"stock_code", "stock_name", "date"}]
    for col in numeric_cols:
        out[col] = pd.to_numeric(out[col], errors="coerce")

    out = (
        out.sort_values(["stock_code"], kind="mergesort")
        .drop_duplicates(subset=["stock_code"], keep="last")
        .reset_index(drop=True)
    )
    return out[
        [
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
    ]


def 原子写入Parquet(df: pd.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = output_path.with_suffix(output_path.suffix + ".tmp")
    df.to_parquet(tmp_path, index=False)
    tmp_path.replace(output_path)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="更新全市场股票基本信息快照。")
    parser.add_argument("--output", type=Path, default=默认输出路径, help="输出 parquet 路径")
    parser.add_argument("--min-rows", type=int, default=默认最小行数, help="最少有效股票行数")
    args = parser.parse_args(argv)

    started = time.time()
    snapshot_date = pd.Timestamp.now(tz="Asia/Seoul").date().isoformat()
    log(f"[start] snapshot_date={snapshot_date}")
    universe = _读取股票宇宙()
    log(f"[universe] codes={len(universe)}")

    rows: list[dict[str, object]] = []
    missing_symbols: list[str] = []
    symbols = universe["symbol"].tolist()
    for start_idx in range(0, len(symbols), 批大小):
        batch_symbols = symbols[start_idx : start_idx + 批大小]
        lines = _抓取腾讯批量行情(batch_symbols)
        seen_symbols: set[str] = set()
        for line in lines:
            item = _解析腾讯行情行(line, snapshot_date)
            if not item:
                continue
            rows.append(item)
            seen_symbols.add(_代码转腾讯符号(str(item["stock_code"])))
        missing_symbols.extend([symbol for symbol in batch_symbols if symbol not in seen_symbols])
        if (start_idx // 批大小 + 1) % 10 == 0 or start_idx + 批大小 >= len(symbols):
            log(f"[quote] {min(start_idx + 批大小, len(symbols))}/{len(symbols)}")

    retry_rows: list[dict[str, object]] = []
    if missing_symbols:
        log(f"[retry] missing_symbols={len(missing_symbols)}")
        for symbol in missing_symbols:
            lines = _抓取腾讯批量行情([symbol])
            for line in lines:
                item = _解析腾讯行情行(line, snapshot_date)
                if item:
                    retry_rows.append(item)
                    break

    raw = pd.DataFrame(rows + retry_rows)
    frame = 整理基本信息快照(raw, snapshot_date)
    frame = universe[["stock_code", "stock_name"]].merge(frame, on="stock_code", how="left", suffixes=("_universe", ""))
    frame["stock_name"] = frame["stock_name"].fillna(frame["stock_name_universe"])
    frame.drop(columns=["stock_name_universe"], inplace=True)
    frame["date"] = frame["date"].fillna(snapshot_date)
    frame = frame[
        [
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
    ]

    if len(frame) < args.min_rows:
        raise SystemExit(f"基本信息有效行数过少: {len(frame)} < {args.min_rows}")

    if frame["total_market_cap"].notna().sum() < args.min_rows:
        raise SystemExit("基本信息中的总市值非空行数过少，判定为异常")

    if frame["float_market_cap"].notna().sum() < args.min_rows:
        raise SystemExit("基本信息中的流通市值非空行数过少，判定为异常")

    原子写入Parquet(frame, args.output.resolve())
    elapsed = time.time() - started
    log(
        f"[done] rows={len(frame)} total_market_cap_non_null={int(frame['total_market_cap'].notna().sum())} "
        f"float_market_cap_non_null={int(frame['float_market_cap'].notna().sum())} elapsed={elapsed:.1f}s "
        f"output={args.output.resolve()}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
