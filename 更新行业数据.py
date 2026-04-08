"""行业板块数据更新脚本。

维护三个 Parquet 文件：
- 数据/申万行业日线数据.parquet       : 31个申万一级行业日线 OHLCV（1999至今）
- 数据/申万二级行业日线数据.parquet   : 124个申万二级行业日线 OHLCV（1999至今）
- 数据/申万行业成分股.parquet         : 全市场个股→申万1/2/3级行业归属映射

用法：
    python3 更新行业数据.py                 # 增量更新申万日线 + 申万成分股
    python3 更新行业数据.py --full          # 强制全量重建
    python3 更新行业数据.py --only sw       # 只更新申万一级/二级日线
    python3 更新行业数据.py --only cons     # 只更新申万成分股映射
    python3 更新行业数据.py --dry-run       # 只打印计划，不写文件
"""

from __future__ import annotations

import argparse
import datetime
import json
import random
import time
from io import StringIO
from pathlib import Path

import akshare as ak
import pandas as pd
import py_mini_racer
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter

from 数据清洗规则 import 原子写入Parquet

项目目录 = Path(__file__).resolve().parent
数据目录 = 项目目录 / "数据"

同花顺日线路径 = 数据目录 / "同花顺行业日线数据.parquet"
申万一级日线路径 = 数据目录 / "申万行业日线数据.parquet"
申万二级日线路径 = 数据目录 / "申万二级行业日线数据.parquet"
申万成分股路径 = 数据目录 / "申万行业成分股.parquet"

THS_开始日期 = "20100101"
SW_开始日期 = "19990101"
同花顺请求间隔 = 2.5  # 秒，避免过快触发同花顺限流
申万请求间隔 = 0.8
请求超时 = 20
同花顺最大重试 = 3
申万最大重试 = 3
同花顺板块最小间隔 = 3.5
同花顺板块最大间隔 = 8.0
同花顺封禁关键字 = (
    "访问过于频繁",
    "验证",
    "验证码",
    "access denied",
    "forbidden",
    "verify",
    "robot",
)


def log(msg: str) -> None:
    now = datetime.datetime.now().strftime("%H:%M:%S")
    try:
        print(f"[{now}] {msg}", flush=True)
    except UnicodeEncodeError:
        print(f"[{now}] {msg.encode('gbk', errors='replace').decode('gbk')}", flush=True)


def 今日字符串() -> str:
    return datetime.date.today().strftime("%Y%m%d")


def 请求后暂停(base: float) -> None:
    time.sleep(base + random.uniform(0.2, 0.8))


def _创建会话() -> requests.Session:
    session = requests.Session()
    try:
        from urllib3.util.retry import Retry

        retry = Retry(
            total=3,
            connect=3,
            read=3,
            status=3,
            backoff_factor=0.8,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["GET"]),
            respect_retry_after_header=True,
        )
        adapter = HTTPAdapter(max_retries=retry, pool_connections=4, pool_maxsize=4)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
    except Exception:
        pass
    return session


def _请求文本(session: requests.Session, url: str, headers: dict[str, str], timeout: int) -> requests.Response:
    return session.get(url, headers=headers, timeout=timeout)


def _同花顺响应疑似封禁(status_code: int, text: str) -> bool:
    if status_code in {401, 403, 418, 429}:
        return True
    lower = text.lower()
    return any(key in lower for key in 同花顺封禁关键字)


def 获取同花顺v_token() -> str:
    from akshare.stock_feature.stock_board_industry_ths import _get_file_content_ths

    js_code = py_mini_racer.MiniRacer()
    js_content = _get_file_content_ths("ths.js")
    js_code.eval(js_content)
    return js_code.call("v")


def 构造同花顺headers(v_token: str) -> dict[str, str]:
    return {
        "Cookie": f"v={v_token}",
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Referer": "https://q.10jqka.com.cn/thshy/",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    }


def 获取本地同花顺板块列表() -> pd.DataFrame:
    if 同花顺日线路径.exists():
        df = pd.read_parquet(同花顺日线路径, columns=["board_name", "board_code"]).drop_duplicates()
        return df.sort_values("board_code").reset_index(drop=True)
    raise FileNotFoundError("本地同花顺行业日线缓存不存在")


def 读取已有最大日期(path: Path, 按列: str = "board_code") -> dict[str, str]:
    """返回 {board_code: max_date} 字典，用于增量更新判断。"""
    if not path.exists():
        return {}
    try:
        df = pd.read_parquet(path, columns=[按列, "date"])
        return df.groupby(按列)["date"].max().to_dict()
    except Exception:
        return {}


def 下载申万行业列表(info_func) -> pd.DataFrame:
    df = info_func()
    return df[["行业代码", "行业名称"]].rename(columns={"行业代码": "board_code", "行业名称": "board_name"})


# ─────────────────────────────────────────────
# 同花顺行业日线
# ─────────────────────────────────────────────

def 获取同花顺板块列表(session: requests.Session) -> pd.DataFrame:
    last_error: Exception | None = None
    for attempt in range(1, 同花顺最大重试 + 1):
        try:
            v_token = 获取同花顺v_token()
            headers = 构造同花顺headers(v_token)
            response = _请求文本(
                session,
                "https://q.10jqka.com.cn/thshy/detail/code/881272/",
                headers=headers,
                timeout=请求超时,
            )
            if _同花顺响应疑似封禁(response.status_code, response.text):
                raise ValueError("同花顺返回疑似封禁页，稍后再试")
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "lxml")
            cate_inner = soup.find(name="div", attrs={"class": "cate_inner"})
            if cate_inner is None:
                raise ValueError("页面缺少 cate_inner，疑似被限流或页面结构变化")
            rows: list[dict[str, str]] = []
            for item in cate_inner.find_all("a"):
                href = str(item.get("href", "")).strip("/")
                parts = href.split("/")
                if len(parts) < 2:
                    continue
                board_code = parts[-1] if parts[-1] else parts[-2]
                board_name = item.get_text(strip=True)
                if board_code.isdigit() and board_name:
                    rows.append({"board_name": board_name, "board_code": board_code})
            if not rows:
                raise ValueError("板块列表为空，疑似被限流或页面结构变化")
            df = pd.DataFrame(rows).drop_duplicates(subset=["board_code"]).reset_index(drop=True)
            return df
        except Exception as e:
            last_error = e
            log(f"获取同花顺板块列表重试 {attempt}/{同花顺最大重试} 失败: {e}")
            if attempt < 同花顺最大重试:
                time.sleep(min(同花顺板块最大间隔, 同花顺板块最小间隔 * attempt))

    log(f"同花顺在线板块列表获取失败，回退本地缓存: {last_error}")
    return 获取本地同花顺板块列表()


def _解析同花顺年线数据(text: str) -> pd.DataFrame:
    start = text.find("{")
    if start < 0:
        raise ValueError("返回内容中缺少 JSON 数据段")
    end = text.rfind(")")
    if end <= start:
        end = len(text)
    payload = json.loads(text[start:end].rstrip(" ;\n"))
    raw = payload.get("data", "")
    if not raw:
        return pd.DataFrame()
    rows = [item.split(",") for item in raw.split(";") if item]
    if not rows:
        return pd.DataFrame()
    max_cols = max(len(item) for item in rows)
    if max_cols < 7:
        raise ValueError("同花顺返回列数不足")
    df = pd.DataFrame(rows)
    df = df.iloc[:, :7]
    df.columns = ["date", "open", "high", "low", "close", "volume", "amount"]
    return df


def 下载同花顺单板块(
    session: requests.Session,
    board_name: str,
    board_code: str,
    start_date: str,
) -> pd.DataFrame | None:
    current_year = datetime.date.today().year
    begin_year = int(start_date[:4])
    end_date = 今日字符串()
    v_token = 获取同花顺v_token()
    headers = 构造同花顺headers(v_token)
    chunks: list[pd.DataFrame] = []

    for year in range(begin_year, current_year + 1):
        url = f"https://d.10jqka.com.cn/v4/line/bk_{board_code}/01/{year}.js"
        last_error: Exception | None = None
        for attempt in range(1, 同花顺最大重试 + 1):
            try:
                response = _请求文本(session, url, headers=headers, timeout=请求超时)
                if _同花顺响应疑似封禁(response.status_code, response.text):
                    raise ValueError("同花顺年线接口返回疑似封禁页")
                response.raise_for_status()
                temp_df = _解析同花顺年线数据(response.text)
                if not temp_df.empty:
                    chunks.append(temp_df)
                break
            except Exception as e:
                last_error = e
                if attempt < 同花顺最大重试 and _同花顺响应疑似封禁(getattr(getattr(e, "response", None), "status_code", 0), str(e)):
                    time.sleep(min(同花顺板块最大间隔, 同花顺板块最小间隔 * attempt * 2))
                elif attempt < 同花顺最大重试:
                    time.sleep(attempt * 2)
                else:
                    log(f"  THS 下载失败 {board_name}({board_code}) {year}: {e}")
                    return None
        if last_error is None:
            请求后暂停(max(同花顺请求间隔, 同花顺板块最小间隔))

    if not chunks:
        return pd.DataFrame()

    df = pd.concat(chunks, ignore_index=True).drop_duplicates(subset=["date"], keep="last")
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])
    df = df[(df["date"] >= pd.to_datetime(start_date)) & (df["date"] <= pd.to_datetime(end_date))]
    if df.empty:
        return pd.DataFrame()

    df["board_name"] = board_name
    df["board_code"] = str(board_code)
    df["date"] = df["date"].dt.strftime("%Y-%m-%d")

    for col in ["open", "high", "low", "close"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").round(4)
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").astype("Int64")
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce").round(2)

    return df[["board_code", "board_name", "date", "open", "high", "low", "close", "volume", "amount"]]


def 更新同花顺行业日线(full: bool = False, dry_run: bool = False) -> bool:
    log("── 同花顺行业日线 ──────────────────────────")
    session = _创建会话()
    try:
        板块列表 = 获取同花顺板块列表(session)
    except Exception as e:
        log(f"获取板块列表失败: {e}")
        return False

    log(f"共 {len(板块列表)} 个板块")

    已有最大日期 = {} if full else 读取已有最大日期(同花顺日线路径, "board_code")
    已有数据: pd.DataFrame | None = None
    if not full and 同花顺日线路径.exists():
        try:
            已有数据 = pd.read_parquet(同花顺日线路径)
        except Exception:
            已有数据 = None

    新增片段: list[pd.DataFrame] = []
    成功, 跳过, 失败 = 0, 0, 0

    for _, row in 板块列表.iterrows():
        name, code = row["board_name"], row["board_code"]
        max_date = 已有最大日期.get(str(code))

        if max_date and max_date >= datetime.date.today().isoformat():
            跳过 += 1
            continue

        # 增量：从已有最大日期次日开始
        if max_date and not full:
            next_day = (datetime.date.fromisoformat(max_date) + datetime.timedelta(days=1)).strftime("%Y%m%d")
            start = next_day
        else:
            start = THS_开始日期

        if dry_run:
            log(f"  [dry-run] {name}({code}) 从 {start} 下载")
            成功 += 1
            continue

        df = 下载同花顺单板块(session, name, str(code), start)
        if df is not None and not df.empty:
            新增片段.append(df)
            成功 += 1
            log(f"  OK  {name}({code}): {len(df)}行 ({df['date'].min()}~{df['date'].max()})")
        else:
            if df is not None:
                跳过 += 1
            else:
                失败 += 1
        请求后暂停(同花顺请求间隔)

    log(f"下载完成: {成功}成功 / {跳过}跳过 / {失败}失败")

    if dry_run:
        return True

    if not 新增片段:
        if 失败 > 0:
            log("本轮全部失败或被限流，文件不变")
            return False
        log("无新数据，文件不变")
        return True

    新增 = pd.concat(新增片段, ignore_index=True)

    if not full and 已有数据 is not None:
        合并 = pd.concat([已有数据, 新增], ignore_index=True)
        合并 = 合并.drop_duplicates(subset=["board_code", "date"], keep="last")
    else:
        合并 = 新增

    合并 = 合并.sort_values(["board_code", "date"]).reset_index(drop=True)
    原子写入Parquet(合并, 同花顺日线路径)
    log(f"已写入 {同花顺日线路径.name}：{len(合并):,} 行，{合并['board_code'].nunique()} 个板块")
    if 失败 > 0:
        log("存在失败板块，本轮仅部分完成，已保存成功部分；建议稍后重试")
        return False
    return True


# ─────────────────────────────────────────────
# 申万行业日线
# ─────────────────────────────────────────────

def 获取申万一级列表() -> pd.DataFrame:
    return 下载申万行业列表(ak.sw_index_first_info)


def 获取申万二级列表() -> pd.DataFrame:
    return 下载申万行业列表(ak.sw_index_second_info)


def 下载申万单行业(code: str, name: str, start_date: str) -> pd.DataFrame | None:
    # 去掉 .SI 后缀，index_hist_sw 用纯数字代码
    pure_code = code.replace(".SI", "")
    try:
        df = ak.index_hist_sw(symbol=pure_code, period="day")
    except Exception as e:
        last_error = e
        for attempt in range(2, 申万最大重试 + 1):
            time.sleep(attempt)
            try:
                df = ak.index_hist_sw(symbol=pure_code, period="day")
                break
            except Exception as retry_error:
                last_error = retry_error
        else:
            log(f"  SW 下载失败 {name}({code}): {last_error}")
            return None

    if df is None or df.empty:
        return pd.DataFrame()

    df = df.rename(columns={
        "日期": "date",
        "开盘": "open",
        "最高": "high",
        "最低": "low",
        "收盘": "close",
        "成交量": "volume",
        "成交额": "amount",
    })
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")

    # 增量过滤
    if start_date:
        start_iso = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:]}"
        df = df[df["date"] >= start_iso]

    if df.empty:
        return pd.DataFrame()

    df["board_code"] = code
    df["board_name"] = name

    for col in ["open", "high", "low", "close"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").round(4)
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce")
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce").round(2)

    return df[["board_code", "board_name", "date", "open", "high", "low", "close", "volume", "amount"]]


def _更新申万级别日线(
    *,
    行业列表: pd.DataFrame,
    目标路径: Path,
    级别名称: str,
    full: bool = False,
    dry_run: bool = False,
) -> bool:
    log(f"── 申万{级别名称}行业日线 ────────────────────────────")
    log(f"共 {len(行业列表)} 个{级别名称}行业")

    已有最大日期 = {} if full else 读取已有最大日期(目标路径, "board_code")
    已有数据: pd.DataFrame | None = None
    if not full and 目标路径.exists():
        try:
            已有数据 = pd.read_parquet(目标路径)
        except Exception:
            已有数据 = None

    新增片段: list[pd.DataFrame] = []
    成功, 跳过, 失败 = 0, 0, 0

    for _, row in 行业列表.iterrows():
        code, name = row["board_code"], row["board_name"]
        max_date = 已有最大日期.get(code)

        if max_date and max_date >= datetime.date.today().isoformat():
            跳过 += 1
            continue

        if max_date and not full:
            next_day = (datetime.date.fromisoformat(max_date) + datetime.timedelta(days=1)).strftime("%Y%m%d")
            start = next_day
        else:
            start = SW_开始日期

        if dry_run:
            log(f"  [dry-run] {name}({code}) 从 {start} 下载")
            成功 += 1
            continue

        df = 下载申万单行业(code, name, start)
        if df is not None and not df.empty:
            新增片段.append(df)
            成功 += 1
            log(f"  OK  {name}({code}): {len(df)}行 ({df['date'].min()}~{df['date'].max()})")
        else:
            if df is not None:
                跳过 += 1
            else:
                失败 += 1
        请求后暂停(申万请求间隔)

    log(f"下载完成: {成功}成功 / {跳过}跳过 / {失败}失败")

    if dry_run:
        return True

    if not 新增片段:
        if 失败 > 0:
            log("本轮全部失败或被限流，文件不变")
            return False
        log("无新数据，文件不变")
        return True

    新增 = pd.concat(新增片段, ignore_index=True)

    if not full and 已有数据 is not None:
        合并 = pd.concat([已有数据, 新增], ignore_index=True)
        合并 = 合并.drop_duplicates(subset=["board_code", "date"], keep="last")
    else:
        合并 = 新增

    合并 = 合并.sort_values(["board_code", "date"]).reset_index(drop=True)
    原子写入Parquet(合并, 目标路径)
    log(f"已写入 {目标路径.name}：{len(合并):,} 行，{合并['board_code'].nunique()} 个{级别名称}行业")
    if 失败 > 0:
        log("存在失败行业，本轮仅部分完成，已保存成功部分；建议稍后重试")
        return False
    return True


def 更新申万一级行业日线(full: bool = False, dry_run: bool = False) -> bool:
    try:
        行业列表 = 获取申万一级列表()
    except Exception as e:
        log(f"获取申万一级列表失败: {e}")
        return False
    return _更新申万级别日线(
        行业列表=行业列表,
        目标路径=申万一级日线路径,
        级别名称="一级",
        full=full,
        dry_run=dry_run,
    )


def 更新申万二级行业日线(full: bool = False, dry_run: bool = False) -> bool:
    try:
        行业列表 = 获取申万二级列表()
    except Exception as e:
        log(f"获取申万二级列表失败: {e}")
        return False
    return _更新申万级别日线(
        行业列表=行业列表,
        目标路径=申万二级日线路径,
        级别名称="二级",
        full=full,
        dry_run=dry_run,
    )


# ─────────────────────────────────────────────
# 申万成分股映射
# ─────────────────────────────────────────────

def 更新申万成分股(dry_run: bool = False) -> bool:
    log("── 申万成分股映射 ──────────────────────────")
    try:
        历史分类 = ak.stock_industry_clf_hist_sw()
    except Exception as e:
        log(f"获取申万历史分类失败: {e}")
        return False

    try:
        页面 = requests.get(
            "https://tushare.pro/document/2?doc_id=181",
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=30,
        )
        页面.raise_for_status()
        申万分类表 = pd.read_html(StringIO(页面.text))[0]
    except Exception as e:
        log(f"获取申万分类对照表失败: {e}")
        return False

    try:
        市场基础数据 = pd.read_parquet(
            数据目录 / "全市场历史数据.parquet",
            columns=["stock_code", "stock_name"],
        ).drop_duplicates("stock_code")
    except Exception as e:
        log(f"读取全市场股票列表失败: {e}")
        return False

    历史分类 = 历史分类.copy()
    历史分类["symbol"] = 历史分类["symbol"].astype(str).str.zfill(6)
    历史分类["start_date"] = pd.to_datetime(历史分类["start_date"], errors="coerce")
    历史分类["update_time"] = pd.to_datetime(历史分类["update_time"], errors="coerce")
    历史分类 = (
        历史分类.sort_values(["symbol", "start_date", "update_time"])
        .drop_duplicates("symbol", keep="last")
        .rename(columns={"symbol": "stock_code"})
    )

    申万分类表 = 申万分类表.copy()
    申万分类表["行业代码"] = 申万分类表["行业代码"].astype(str).str.zfill(6)
    申万分类表["指数代码"] = 申万分类表["指数代码"].astype(str).str.zfill(6)
    一级映射 = (
        申万分类表.loc[申万分类表["指数类别"] == "一级行业", ["一级行业", "指数代码"]]
        .drop_duplicates()
        .rename(columns={"指数代码": "sw1_code"})
    )
    二级映射 = (
        申万分类表.loc[
            申万分类表["指数类别"] == "二级行业",
            ["一级行业", "二级行业", "指数代码"],
        ]
        .drop_duplicates()
        .rename(columns={"指数代码": "sw2_code"})
    )

    log(f"历史分类 {len(历史分类):,} 只股票，分类对照表 {len(申万分类表):,} 行，开始合并...")

    if dry_run:
        log(f"[dry-run] 将合并 {len(市场基础数据):,} 只股票的申万分类")
        return True

    合并 = 市场基础数据.merge(
        历史分类[["stock_code", "start_date", "industry_code"]],
        on="stock_code",
        how="left",
        validate="one_to_one",
    )
    合并 = 合并.merge(
        申万分类表,
        left_on="industry_code",
        right_on="行业代码",
        how="left",
        validate="many_to_one",
    )
    合并 = 合并.merge(
        一级映射,
        on="一级行业",
        how="left",
        validate="many_to_one",
    )
    合并 = 合并.merge(
        二级映射,
        on=["一级行业", "二级行业"],
        how="left",
        validate="many_to_one",
    )

    缺失分类 = 合并["指数代码"].isna()
    if 缺失分类.any():
        缺失股票 = 合并.loc[缺失分类, "stock_code"].head(20).tolist()
        log(f"申万分类合并后仍有 {缺失分类.sum():,} 只股票缺失行业信息，示例: {缺失股票}")
        return False

    合并["entry_date"] = pd.to_datetime(合并["start_date"], errors="coerce").dt.date
    合并["sw_level1"] = 合并["一级行业"]
    合并["sw_level2"] = 合并["二级行业"].fillna("")
    合并["sw_level3"] = 合并["三级行业"].fillna("")
    合并["sw3_code"] = 合并["指数代码"].astype(str).str.zfill(6) + ".SI"
    合并["sw3_name"] = 合并["三级行业"]
    合并["sw2_code"] = 合并["sw2_code"].astype(str).str.zfill(6) + ".SI"
    合并["sw1_code"] = 合并["sw1_code"].astype(str).str.zfill(6) + ".SI"
    合并["sw2_name"] = 合并["二级行业"].replace("", pd.NA)
    合并["sw1_name"] = 合并["一级行业"]
    合并 = 合并[
        [
            "stock_code",
            "stock_name",
            "entry_date",
            "sw_level1",
            "sw_level2",
            "sw_level3",
            "sw3_code",
            "sw3_name",
            "sw2_name",
            "sw2_code",
            "sw1_name",
            "sw1_code",
        ]
    ].copy()
    合并["stock_code"] = 合并["stock_code"].astype(str).str.zfill(6)
    合并 = 合并.drop_duplicates(subset=["stock_code"])
    合并 = 合并.sort_values("stock_code").reset_index(drop=True)

    全市场股票数 = len(市场基础数据)
    覆盖股票数 = 合并["stock_code"].nunique()
    三级行业数 = 合并["sw3_code"].nunique()
    一级行业数 = 合并["sw1_code"].nunique()
    二级行业数 = 合并["sw2_code"].nunique()
    覆盖率 = 覆盖股票数 / 全市场股票数 if 全市场股票数 else None

    最低覆盖率 = 0.95
    最低股票数 = 1000
    最低三级行业数 = 100
    if (
        覆盖股票数 < 最低股票数
        or 三级行业数 < 最低三级行业数
        or (覆盖率 is not None and 覆盖率 < 最低覆盖率)
    ):
        log(
            f"申万成分股结果覆盖率过低，拒绝覆盖现有文件: "
            f"stocks={覆盖股票数} sw1={一级行业数} sw2={二级行业数} sw3={三级行业数} "
            f"market={全市场股票数} coverage={覆盖率:.2%}"
        )
        return False

    原子写入Parquet(合并, 申万成分股路径)
    log(
        f"已写入 {申万成分股路径.name}：{len(合并):,} 行，"
        f"覆盖 {覆盖股票数} 只股票，{一级行业数}/{二级行业数}/{三级行业数} 个一/二/三级行业"
    )
    return True


# ─────────────────────────────────────────────
# 主入口
# ─────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(description="行业板块数据更新（申万）")
    parser.add_argument("--full", action="store_true", help="强制全量重建")
    parser.add_argument("--only", choices=["sw", "cons"], help="只更新指定部分")
    parser.add_argument("--dry-run", action="store_true", help="只打印计划，不写文件")
    args = parser.parse_args()

    数据目录.mkdir(parents=True, exist_ok=True)
    results = []

    run_sw = args.only in (None, "sw")
    run_cons = args.only in (None, "cons")

    if run_sw:
        ok1 = 更新申万一级行业日线(full=args.full, dry_run=args.dry_run)
        results.append(("申万一级行业日线", ok1))
        ok2 = 更新申万二级行业日线(full=args.full, dry_run=args.dry_run)
        results.append(("申万二级行业日线", ok2))

    if run_cons:
        ok = 更新申万成分股(dry_run=args.dry_run)
        results.append(("申万成分股", ok))

    log("── 汇总 ────────────────────────────────────")
    all_ok = True
    for name, ok in results:
        status = "OK" if ok else "FAIL"
        log(f"  {status}  {name}")
        if not ok:
            all_ok = False

    return 0 if all_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
