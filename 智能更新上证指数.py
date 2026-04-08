"""上证指数多周期增量更新脚本。

用途：
- 维护上证指数日线、15分钟、30分钟、60分钟和月线 Parquet 文件
- 日线和分钟线按最近完整交易日更新
- 月线由日线聚合生成，确保当前月会随日线一起滚动
"""

from __future__ import annotations

import argparse
import datetime
import io
import os
import sys
import tempfile
import time
from pathlib import Path

import baostock as bs
import akshare as ak
import pandas as pd
import requests

from 数据清洗规则 import 原子写入Parquet


项目目录 = Path(__file__).resolve().parent
BAOSTOCK_LOCK = Path(tempfile.gettempdir()) / "baostock.lock"

上证指数代码 = "sh.000001"
上证指数名称 = "上证指数"

上证指数日线路径 = 项目目录 / "数据" / "上证指数日线数据.parquet"
上证指数15分钟路径 = 项目目录 / "数据" / "上证指数15分钟数据.parquet"
上证指数30分钟路径 = 项目目录 / "数据" / "上证指数30分钟数据.parquet"
上证指数60分钟路径 = 项目目录 / "数据" / "上证指数60分钟数据.parquet"
上证指数月线路径 = 项目目录 / "数据" / "上证指数月线数据.parquet"

BAOSTOCK_FIELDS_日线 = "date,open,high,low,close,volume,amount,turn"
BAOSTOCK_FIELDS_分钟 = "date,time,code,open,high,low,close,volume,amount"

日线开始日期 = "1990-01-01"
分钟开始日期 = "2020-01-01"
最大重试次数 = 3
重试基础间隔 = 1
东财分钟最大重试次数 = 3
新浪分钟后备符号 = "sh000001"

日线列名 = [
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

分钟列名 = [
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


def log(msg: str) -> None:
    try:
        print(msg, flush=True)
    except UnicodeEncodeError:
        print(msg.encode("gbk", errors="replace").decode("gbk"), flush=True)


def _完整交易日截止时间() -> datetime.time:
    return datetime.time(18, 0)


def _规范化日期(value: object) -> str | None:
    if value is None or pd.isna(value):
        return None
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        text = str(value).strip()
        return text[:10] if len(text) >= 10 else None
    return ts.strftime("%Y-%m-%d")


def _标准化时间(s: pd.Series) -> pd.Series:
    x = s.fillna("").astype(str).str.strip()

    mask_long = x.str.fullmatch(r"\d{14,17}", na=False)
    if mask_long.any():
        xx = x.loc[mask_long]
        x.loc[mask_long] = xx.str.slice(8, 10) + ":" + xx.str.slice(10, 12) + ":" + xx.str.slice(12, 14)

    mask_hhmm = x.str.fullmatch(r"\d{2}:\d{2}", na=False)
    if mask_hhmm.any():
        x.loc[mask_hhmm] = x.loc[mask_hhmm] + ":00"

    mask_ok = x.str.fullmatch(r"\d{2}:\d{2}:\d{2}", na=False)
    x.loc[~mask_ok] = ""
    return x


def _检查锁() -> bool:
    if BAOSTOCK_LOCK.exists():
        mtime = BAOSTOCK_LOCK.stat().st_mtime
        if time.time() - mtime < 1800:
            return True
        BAOSTOCK_LOCK.unlink(missing_ok=True)
    return False


def _创建锁() -> None:
    BAOSTOCK_LOCK.parent.mkdir(parents=True, exist_ok=True)
    BAOSTOCK_LOCK.write_text(f"index-update {datetime.datetime.now().isoformat()}", encoding="utf-8")


def _释放锁() -> None:
    BAOSTOCK_LOCK.unlink(missing_ok=True)


def _静默登录() -> object:
    old_stdout = sys.stdout
    sys.stdout = io.StringIO()
    try:
        return bs.login()
    finally:
        sys.stdout = old_stdout


def _静默登出() -> None:
    old_stdout = sys.stdout
    sys.stdout = io.StringIO()
    try:
        bs.logout()
    except Exception:
        pass
    finally:
        sys.stdout = old_stdout


def _获取最近完整交易日() -> str | None:
    today = datetime.date.today()
    start = (today - datetime.timedelta(days=40)).strftime("%Y-%m-%d")
    end = today.strftime("%Y-%m-%d")

    rs = bs.query_trade_dates(start_date=start, end_date=end)
    trade_days: list[str] = []
    while rs.next():
        day, is_trade = rs.get_row_data()
        if is_trade == "1":
            trade_days.append(day)

    if not trade_days:
        return None

    trade_days = sorted(trade_days)
    cutoff = _完整交易日截止时间()
    if datetime.datetime.now().time() < cutoff and trade_days[-1] == today.strftime("%Y-%m-%d"):
        trade_days = trade_days[:-1]

    if not trade_days:
        return None
    return trade_days[-1]


def _读取文件最大日期(path: Path) -> str | None:
    if not path.exists():
        return None

    try:
        df = pd.read_parquet(path, columns=["date"])
    except Exception:
        return None

    if df.empty or "date" not in df.columns:
        return None

    dt = pd.to_datetime(df["date"], errors="coerce")
    dt = dt.dropna()
    if dt.empty:
        return None
    return dt.max().strftime("%Y-%m-%d")


def _读取日线文件(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=日线列名)

    df = pd.read_parquet(path)
    if df.empty:
        return pd.DataFrame(columns=日线列名)

    required = {"index_code", "index_name", "date"}
    if not required.issubset(df.columns):
        raise ValueError(f"日线文件列不完整: {path}")

    out = df.copy()
    out["index_code"] = out["index_code"].astype(str).str.strip()
    out["index_name"] = out["index_name"].fillna("").astype(str).str.strip()
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out = out[out["date"].notna()].copy()
    for col in ["open", "high", "low", "close", "volume", "amount", "turnover"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")
    out = out.sort_values("date", kind="mergesort").drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)
    out["date"] = out["date"].dt.strftime("%Y-%m-%d")
    return out[日线列名]


def _读取分钟文件(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=分钟列名)

    df = pd.read_parquet(path)
    if df.empty:
        return pd.DataFrame(columns=分钟列名)

    required = {"index_code", "index_name", "date", "time"}
    if not required.issubset(df.columns):
        raise ValueError(f"分钟文件列不完整: {path}")

    out = df.copy()
    out["index_code"] = out["index_code"].astype(str).str.strip()
    out["index_name"] = out["index_name"].fillna("").astype(str).str.strip()
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out["time"] = _标准化时间(out["time"])
    out = out[out["date"].notna() & (out["time"] != "")].copy()
    for col in ["open", "high", "low", "close", "volume", "amount"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")
    out = out.sort_values(["date", "time"], kind="mergesort").drop_duplicates(subset=["date", "time"], keep="last").reset_index(drop=True)
    out["date"] = out["date"].dt.strftime("%Y-%m-%d")
    return out[分钟列名]


def _转日线格式(rows: list[list[str]], fields: list[str]) -> pd.DataFrame:
    df = pd.DataFrame(rows, columns=fields)
    out = pd.DataFrame(index=df.index)
    out["index_code"] = 上证指数代码
    out["index_name"] = 上证指数名称
    out["date"] = df["date"]
    for col in ["open", "high", "low", "close", "volume", "amount"]:
        out[col] = pd.to_numeric(df[col], errors="coerce")
    out["turnover"] = pd.to_numeric(df["turn"], errors="coerce")
    out = out.sort_values("date", kind="mergesort").drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)
    return out[日线列名]


def _转分钟格式(rows: list[list[str]], fields: list[str]) -> pd.DataFrame:
    df = pd.DataFrame(rows, columns=fields)
    out = pd.DataFrame(index=df.index)
    out["index_code"] = 上证指数代码
    out["index_name"] = 上证指数名称
    out["date"] = pd.to_datetime(df["date"], errors="coerce")
    out["time"] = _标准化时间(df["time"])
    for col in ["open", "high", "low", "close", "volume", "amount"]:
        out[col] = pd.to_numeric(df[col], errors="coerce")
    out = out[out["date"].notna() & (out["time"] != "")].copy()
    out = out.sort_values(["date", "time"], kind="mergesort").drop_duplicates(subset=["date", "time"], keep="last").reset_index(drop=True)
    out["date"] = out["date"].dt.strftime("%Y-%m-%d")
    return out[分钟列名]


def _聚合日线为月线(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=日线列名)

    out = df.copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out = out[out["date"].notna()].copy()
    if out.empty:
        return pd.DataFrame(columns=日线列名)

    for col in ["open", "high", "low", "close", "volume", "amount", "turnover"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")

    out = out.sort_values("date", kind="mergesort").drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)
    out["month"] = out["date"].dt.to_period("M")
    monthly = out.groupby("month", sort=True, as_index=False).agg(
        index_code=("index_code", "last"),
        index_name=("index_name", "last"),
        date=("date", "max"),
        open=("open", "first"),
        high=("high", "max"),
        low=("low", "min"),
        close=("close", "last"),
        volume=("volume", "sum"),
        amount=("amount", "sum"),
        turnover=("turnover", "sum"),
    )
    monthly["date"] = monthly["date"].dt.strftime("%Y-%m-%d")
    return monthly[日线列名]


转日线格式 = _转日线格式
转分钟格式 = _转分钟格式
聚合日线为月线 = _聚合日线为月线


def _下载区间(frequency: str, fields: str, start_date: str, end_date: str) -> tuple[pd.DataFrame | None, str | None]:
    rows: list[list[str]] = []
    got_fields: list[str] | None = None
    err: str | None = None

    for i in range(最大重试次数):
        try:
            rs = bs.query_history_k_data_plus(
                code=上证指数代码,
                fields=fields,
                start_date=start_date,
                end_date=end_date,
                frequency=frequency,
                adjustflag="3",
            )
            if rs is None:
                err = "query returned none"
                if i < 最大重试次数 - 1:
                    time.sleep(重试基础间隔 * (2**i))
                    continue
                break
            if rs.error_code != "0":
                err = rs.error_msg
                if "用户未登录" in err:
                    _静默登出()
                    lg = _静默登录()
                    if lg.error_code == "0":
                        continue
                if i < 最大重试次数 - 1:
                    time.sleep(重试基础间隔 * (2**i))
                    continue
                break

            got_fields = rs.fields
            while rs.next():
                rows.append(rs.get_row_data())
            break
        except Exception as e:
            err = str(e)
            if i < 最大重试次数 - 1:
                time.sleep(重试基础间隔 * (2**i))

    if not rows or got_fields is None:
        return None, err or "no data"
    return pd.DataFrame(rows, columns=got_fields), None


def _下载东财分钟(period: str, start_date: str, end_date: str) -> tuple[pd.DataFrame | None, str | None]:
    url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
    params = {
        "secid": "1.000001",
        "ut": "7eea3edcaed734bea9cbfc24409ed989",
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        "klt": period,
        "fqt": "1",
        "beg": "0",
        "end": "20500000",
    }
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Referer": "https://quote.eastmoney.com/center/hszs.html",
        "Origin": "https://quote.eastmoney.com",
        "Accept": "application/json, text/javascript, */*; q=0.01",
    }

    err: str | None = None
    for i in range(东财分钟最大重试次数):
        try:
            r = requests.get(url, params=params, headers={**headers, "Connection": "close"}, timeout=20)
            r.raise_for_status()
            data_json = r.json()
            break
        except Exception as e:
            err = str(e)
            if i < 东财分钟最大重试次数 - 1:
                time.sleep(重试基础间隔 * (2**i))
                continue
            return None, err

    data = data_json.get("data") or {}
    klines = data.get("klines") or []
    if not klines:
        return None, "no data"

    temp_df = pd.DataFrame([item.split(",") for item in klines])
    if temp_df.empty:
        return None, "no data"

    temp_df.columns = [
        "datetime",
        "open",
        "close",
        "high",
        "low",
        "volume",
        "amount",
        "amplitude",
        "pct_change",
        "change",
        "turnover",
    ]
    temp_df["datetime"] = pd.to_datetime(temp_df["datetime"], errors="coerce")
    temp_df = temp_df[temp_df["datetime"].notna()].copy()
    if temp_df.empty:
        return None, "no data"

    start_ts = pd.to_datetime(start_date, errors="coerce")
    end_ts = pd.to_datetime(end_date, errors="coerce")
    if pd.notna(start_ts):
        temp_df = temp_df[temp_df["datetime"] >= start_ts]
    if pd.notna(end_ts):
        end_ts = end_ts + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
        temp_df = temp_df[temp_df["datetime"] <= end_ts]
    if temp_df.empty:
        return None, "no data"

    out = pd.DataFrame(index=temp_df.index)
    out["index_code"] = 上证指数代码
    out["index_name"] = 上证指数名称
    out["date"] = temp_df["datetime"].dt.strftime("%Y-%m-%d")
    out["time"] = temp_df["datetime"].dt.strftime("%H:%M:%S")
    for col in ["open", "high", "low", "close", "volume", "amount"]:
        out[col] = pd.to_numeric(temp_df.get(col), errors="coerce")
    out = out.sort_values(["date", "time"], kind="mergesort").drop_duplicates(subset=["date", "time"], keep="last").reset_index(drop=True)
    return out[分钟列名], None


def _下载分钟兜底(频率: str, target_date: str, 现有_df: pd.DataFrame) -> tuple[pd.DataFrame | None, str | None]:
    fallback_start = target_date
    if not 现有_df.empty and "date" in 现有_df.columns:
        try:
            fallback_start = pd.to_datetime(现有_df["date"], errors="coerce").dropna().max().strftime("%Y-%m-%d")
        except Exception:
            fallback_start = target_date

    log(f"  东财失败后改用新浪分钟兜底: {fallback_start} ~ {target_date}（频率={频率}）")
    try:
        raw = ak.stock_zh_a_minute(symbol=新浪分钟后备符号, period=频率)
    except Exception as e:
        return None, str(e)

    if raw is None or raw.empty:
        return None, "no data"

    temp = raw.copy()
    if "day" not in temp.columns:
        return None, "no data"

    temp["day"] = pd.to_datetime(temp["day"], errors="coerce")
    temp = temp[temp["day"].notna()].copy()
    if temp.empty:
        return None, "no data"

    start_ts = pd.to_datetime(fallback_start, errors="coerce")
    end_ts = pd.to_datetime(target_date, errors="coerce")
    if pd.notna(start_ts):
        temp = temp[temp["day"] >= start_ts]
    if pd.notna(end_ts):
        temp = temp[temp["day"] <= (end_ts + pd.Timedelta(days=1) - pd.Timedelta(seconds=1))]
    if temp.empty:
        return None, "no data"

    out = pd.DataFrame(index=temp.index)
    out["index_code"] = 上证指数代码
    out["index_name"] = 上证指数名称
    out["date"] = temp["day"].dt.strftime("%Y-%m-%d")
    out["time"] = temp["day"].dt.strftime("%H:%M:%S")
    for col in ["open", "high", "low", "close", "volume"]:
        out[col] = pd.to_numeric(temp.get(col), errors="coerce")
    # 新浪分钟源不提供成交额，按均价近似补出，保证与现有 Parquet 结构一致。
    out["amount"] = pd.to_numeric(out["close"], errors="coerce") * pd.to_numeric(out["volume"], errors="coerce") / 3.0
    out = out.sort_values(["date", "time"], kind="mergesort").drop_duplicates(subset=["date", "time"], keep="last").reset_index(drop=True)
    return out[分钟列名], None


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="上证指数多周期增量更新")
    p.add_argument("--dry-run", action="store_true", help="只拉取和校验，不写回文件")
    return p.parse_args()


def _写回或校验(path: Path, df: pd.DataFrame, dry_run: bool, label: str, target_date: str) -> None:
    actual = _规范化日期(df["date"].max()) if not df.empty else None
    if actual != target_date:
        raise SystemExit(f"[错误] {label} 未到目标交易日: actual={actual or '-'} target={target_date}")

    if dry_run:
        log(f"[DRY-RUN] {label} 不写回Parquet")
        return

    原子写入Parquet(df, path)
    latest = _读取文件最大日期(path)
    if latest != target_date:
        raise SystemExit(f"[错误] {label} 写回后最大日期异常: {latest or '-'} != {target_date}")
    log(f"[OK] 已写回: {path}")


def _更新日线(target_date: str, dry_run: bool) -> pd.DataFrame | None:
    现有最后日期 = _读取文件最大日期(上证指数日线路径)
    if 现有最后日期 is not None and 现有最后日期 >= target_date:
        log("[SKIP] 上证指数日线已是最新")
        return None

    log(f"  日线下载区间: {日线开始日期} ~ {target_date}")
    raw, err = _下载区间("d", BAOSTOCK_FIELDS_日线, 日线开始日期, target_date)
    if raw is None:
        raise SystemExit(f"[错误] 未获得上证指数日线数据: {err}")

    df = _转日线格式(raw.values.tolist(), list(raw.columns))
    log(f"  日线新增/重建行数: {len(df):,}")
    _写回或校验(上证指数日线路径, df, dry_run, "上证指数日线", target_date)
    return df


def _更新分钟频率(frequency: str, path: Path, target_date: str, dry_run: bool) -> pd.DataFrame | None:
    现有_df = _读取分钟文件(path)
    if not 现有_df.empty:
        log(f"  {path.name} 现有区间: {现有_df['date'].min()} ~ {现有_df['date'].max()}")
    log(f"  {path.name} 下载区间: 东财可用窗口 ~ {target_date}（频率={frequency}）")
    raw, err = _下载东财分钟(frequency, f"{分钟开始日期} 00:00:00", target_date)
    if raw is None:
        log(f"  [警告] 东财分钟线失败，准备使用 BaoStock 兜底: {err}")
        raw, err = _下载分钟兜底(frequency, target_date, 现有_df)
    if raw is None:
        raise SystemExit(f"[错误] 未获得 {path.name} 数据: {err}")

    df = pd.concat([现有_df, raw], ignore_index=True)
    df = df[分钟列名].copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df["time"] = _标准化时间(df["time"])
    for col in ["open", "high", "low", "close", "volume", "amount"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df[df["date"].notna() & (df["time"] != "")].copy()
    df = df.sort_values(["date", "time"], kind="mergesort").drop_duplicates(subset=["date", "time"], keep="last").reset_index(drop=True)
    log(f"  {path.name} 行数: {len(df):,}")
    _写回或校验(path, df, dry_run, path.name, target_date)
    return df


def _更新月线(daily_df: pd.DataFrame | None, target_date: str, dry_run: bool) -> pd.DataFrame | None:
    现有最后日期 = _读取文件最大日期(上证指数月线路径)
    if 现有最后日期 is not None and 现有最后日期 >= target_date:
        log("[SKIP] 上证指数月线已是最新")
        return None

    if daily_df is None:
        daily_df = _读取日线文件(上证指数日线路径)
    if daily_df.empty:
        raise SystemExit("[错误] 无法构建上证指数月线：日线数据为空")

    monthly_df = _聚合日线为月线(daily_df)
    log(f"  上证指数月线行数: {len(monthly_df):,}")
    _写回或校验(上证指数月线路径, monthly_df, dry_run, "上证指数月线", target_date)
    return monthly_df


def 更新上证指数(dry_run: bool) -> None:
    log("=" * 70)
    log(f"上证指数多周期更新 - {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log("=" * 70)
    log(f"dry_run={dry_run}")

    if _检查锁():
        log(f"[警告] 检测到其他BaoStock脚本运行中: {BAOSTOCK_LOCK}")
        return

    _创建锁()
    try:
        lg = bs.login()
        if lg.error_code != "0":
            log(f"[错误] BaoStock登录失败: {lg.error_msg}")
            return

        try:
            最近完整交易日 = _获取最近完整交易日()
            if not 最近完整交易日:
                log("[错误] 未获取到最近完整交易日")
                return

            log(f"  目标交易日: {最近完整交易日}")

            failures: list[str] = []
            daily_df: pd.DataFrame | None = None

            try:
                daily_df = _更新日线(最近完整交易日, dry_run)
            except SystemExit as e:
                failures.append(str(e))

            for frequency, path in [("15", 上证指数15分钟路径), ("30", 上证指数30分钟路径), ("60", 上证指数60分钟路径)]:
                try:
                    _更新分钟频率(frequency, path, 最近完整交易日, dry_run)
                except SystemExit as e:
                    failures.append(str(e))

            try:
                _更新月线(daily_df, 最近完整交易日, dry_run)
            except SystemExit as e:
                failures.append(str(e))

            if failures:
                log("[ERROR] 上证指数多周期更新存在失败项:")
                for item in failures:
                    log(f"  {item}")
                raise SystemExit(2)
        finally:
            _静默登出()
    finally:
        _释放锁()


def main() -> None:
    args = parse_args()
    更新上证指数(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
