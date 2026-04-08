"""抓取同花顺行业板块成分股。

从同花顺行情中心页面解析各行业板块的完整成分股列表，
输出到 数据/同花顺行业成分股.parquet。

原理：
- 第一页：使用 AkShare 内置 JS token（v=xxx），无需账号
- 分页（第2页起）：需要登录态 Cookie，否则返回 401
- Cookie 保存在 进度/ths_cookie.txt，过期后重新从浏览器复制

用法：
    python3 抓取同花顺成分股.py                      # 使用已保存的 Cookie
    python3 抓取同花顺成分股.py --cookie "..."        # 传入新 Cookie
    python3 抓取同花顺成分股.py --dry-run             # 只打印计划
    python3 抓取同花顺成分股.py --interval 5          # 自定义请求间隔（秒，默认4.5）
"""

from __future__ import annotations

import argparse
import datetime
import time
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
成分股路径 = 数据目录 / "同花顺行业成分股.parquet"

默认请求间隔 = 4.5  # 秒，默认放慢一点，减少被同花顺限流的概率
封禁关键字 = (
    "访问过于频繁",
    "验证码",
    "verify",
    "forbidden",
    "access denied",
    "robot",
)


def log(msg: str) -> None:
    now = datetime.datetime.now().strftime("%H:%M:%S")
    try:
        print(f"[{now}] {msg}", flush=True)
    except UnicodeEncodeError:
        print(f"[{now}] {msg.encode('gbk', errors='replace').decode('gbk')}", flush=True)


def 获取v_token() -> str:
    """使用 AkShare 内置 JS 生成同花顺的 v token，无需账号登录。"""
    from akshare.stock_feature.stock_board_industry_ths import _get_file_content_ths
    js_code = py_mini_racer.MiniRacer()
    js_content = _get_file_content_ths("ths.js")
    js_code.eval(js_content)
    return js_code.call("v")


def 构造headers(v_token: str) -> dict:
    return {
        "Cookie": f"v={v_token}",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/89.0.4389.90 Safari/537.36"
        ),
        "Referer": "https://q.10jqka.com.cn/thshy/",
    }


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


def _疑似封禁(status_code: int, text: str) -> bool:
    if status_code in {401, 403, 418, 429}:
        return True
    lower = text.lower()
    return any(key in lower for key in 封禁关键字)


def _请求(session: requests.Session, url: str, headers: dict, timeout: int = 15) -> requests.Response:
    return session.get(url, headers=headers, timeout=timeout)


def 解析表格(html: str) -> tuple[list[dict], int]:
    """解析页面 HTML，返回 (成分股记录列表, 总页数)。"""
    soup = BeautifulSoup(html, "lxml")

    # 解析总页数
    page_info = soup.find("span", class_="page_info")
    total_pages = 1
    if page_info:
        try:
            total_pages = int(page_info.text.strip().split("/")[-1])
        except Exception:
            pass

    table = soup.find("table")
    if table is None:
        return [], total_pages

    rows = table.find_all("tr")
    if len(rows) < 2:
        return [], total_pages

    col_names = [th.get_text(strip=True) for th in rows[0].find_all(["th", "td"])]
    records = []
    for row in rows[1:]:
        cells = row.find_all("td")
        if not cells:
            continue
        record = {col_names[i]: cells[i].get_text(strip=True) for i in range(min(len(col_names), len(cells)))}
        records.append(record)

    return records, total_pages


def 抓取单板块成分股(
    session: requests.Session,
    board_code: str,
    board_name: str,
    v_headers: dict,
    login_headers: dict | None,
    间隔: float,
) -> pd.DataFrame | None:
    # 第 1 页用 v token（同时获取总页数）
    url = f"https://q.10jqka.com.cn/thshy/detail/code/{board_code}/"
    try:
        r = _请求(session, url, headers=v_headers, timeout=15)
    except Exception as e:
        log(f"  请求失败 {board_name}({board_code}): {e}")
        return None

    if _疑似封禁(r.status_code, r.text):
        log(f"  疑似被限流/封禁 {board_name}({board_code})，暂停本板块")
        return None

    if r.status_code != 200:
        log(f"  HTTP {r.status_code}  {board_name}({board_code})")
        return None

    all_records, total_pages = 解析表格(r.text)
    if not all_records and total_pages <= 1:
        log(f"  页面结构异常或返回空表 {board_name}({board_code})")
        return None

    # 分页用登录 Cookie（AJAX 接口需要登录态）
    page_hdrs = login_headers if login_headers else v_headers
    for page in range(2, total_pages + 1):
        time.sleep(间隔 + 1.5)
        page_url = f"https://q.10jqka.com.cn/thshy/detail/code/{board_code}/page/{page}/ajax/1/"
        try:
            pr = _请求(session, page_url, headers=page_hdrs, timeout=15)
            if _疑似封禁(pr.status_code, pr.text):
                log(f"  分页疑似被限流/封禁 {board_name} p{page}，停止该板块")
                break
            if pr.status_code == 200:
                records, _ = 解析表格(pr.text)
                all_records.extend(records)
            else:
                log(f"  分页 HTTP {pr.status_code}  {board_name} p{page}（需要登录Cookie）")
                break  # 后续页也会失败，直接停
        except Exception as e:
            log(f"  分页请求失败 {board_name} p{page}: {e}")

    if not all_records:
        return None

    df = pd.DataFrame(all_records)

    rename_map = {
        "代码": "stock_code",
        "名称": "stock_name",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    if "stock_code" not in df.columns:
        return None

    df = df[["stock_code", "stock_name"]].copy()
    df["board_code"] = str(board_code)
    df["board_name"] = board_name

    df["stock_code"] = df["stock_code"].astype(str).str.zfill(6)
    df = df[df["stock_code"].str.match(r"^\d{6}$")]

    return df if not df.empty else None


COOKIE文件 = 项目目录 / "进度" / "ths_cookie.txt"


def 加载login_cookie(args) -> str | None:
    if hasattr(args, "cookie") and args.cookie:
        cookie = args.cookie.strip()
        COOKIE文件.parent.mkdir(parents=True, exist_ok=True)
        COOKIE文件.write_text(cookie, encoding="utf-8")
        return cookie
    if COOKIE文件.exists():
        return COOKIE文件.read_text(encoding="utf-8").strip()
    return None


def 构造login_headers(cookie: str) -> dict:
    return {
        "Cookie": cookie,
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Referer": "https://q.10jqka.com.cn/thshy/",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="抓取同花顺行业板块完整成分股")
    parser.add_argument("--cookie", help="登录Cookie（用于翻页，过期后重新复制）")
    parser.add_argument("--dry-run", action="store_true", help="只打印计划，不抓取")
    parser.add_argument("--interval", type=float, default=默认请求间隔, help=f"请求间隔秒数（默认{默认请求间隔}）")
    args = parser.parse_args()

    日线路径 = 数据目录 / "同花顺行业日线数据.parquet"
    session = _创建会话()
    try:
        板块列表 = ak.stock_board_industry_name_ths()
        板块列表.columns = ["board_name", "board_code"]
    except Exception as e:
        log(f"从同花顺获取板块列表失败: {e}")
        if 日线路径.exists():
            log("使用本地日线数据中的板块列表作为备用")
            tmp = pd.read_parquet(日线路径, columns=["board_code", "board_name"]).drop_duplicates()
            板块列表 = tmp.rename(columns={"board_name": "board_name", "board_code": "board_code"})
        else:
            log("无本地备用板块列表，退出")
            return 1

    log(f"共 {len(板块列表)} 个板块，请求间隔 {args.interval}s")

    if args.dry_run:
        for _, row in 板块列表.iterrows():
            log(f"  [dry-run] {row['board_name']}({row['board_code']})")
        return 0

    log("生成 v token（用于首页）...")
    try:
        v_token = 获取v_token()
        log(f"v token OK")
    except Exception as e:
        log(f"生成 v token 失败: {e}")
        return 1

    v_hdrs = 构造headers(v_token)

    login_cookie = 加载login_cookie(args)
    if login_cookie:
        login_hdrs = 构造login_headers(login_cookie)
        log("登录 Cookie 已加载（用于翻页）")
    else:
        login_hdrs = None
        log("未提供登录 Cookie，每个板块只能获取第1页（最多20只）")
        log("提示：用 --cookie 参数传入浏览器 Cookie 可获取完整成分股")

    # 读取已有数据，跳过已抓成功的板块（断点续传）
    已有数据: pd.DataFrame | None = None
    已完成板块: set[str] = set()
    if 成分股路径.exists():
        try:
            已有数据 = pd.read_parquet(成分股路径)
            已完成板块 = set(已有数据["board_code"].astype(str).unique())
            log(f"断点续传：已有 {len(已完成板块)} 个板块，跳过")
        except Exception:
            pass

    片段: list[pd.DataFrame] = []
    成功, 跳过, 失败 = 0, 0, 0
    连续失败 = 0

    for _, row in 板块列表.iterrows():
        name, code = row["board_name"], str(row["board_code"])

        if code in 已完成板块:
            跳过 += 1
            continue

        df = 抓取单板块成分股(session, code, name, v_hdrs, login_hdrs, args.interval)
        if df is not None:
            片段.append(df)
            成功 += 1
            连续失败 = 0
            log(f"  OK  {name}({code}): {len(df)} 只股票")

            # 每成功5个板块就写一次，保留进度
            if 成功 % 5 == 0:
                _写入中间结果(片段, 已有数据, 成分股路径)
                log(f"  [进度保存] 已完成 {len(已完成板块) + 成功} 个板块")
        else:
            失败 += 1
            连续失败 += 1
            log(f"  FAIL {name}({code})")

            # 连续失败3次说明被封，立即保存已有进度并退出
            if 连续失败 >= 3:
                log(f"连续失败 {连续失败} 次，可能被限速，保存进度后退出")
                log("等待一段时间后重新运行即可从断点继续")
                break

        time.sleep(args.interval)

    log(f"本轮完成: {成功}成功 / {跳过}跳过 / {失败}失败")
    _写入中间结果(片段, 已有数据, 成分股路径)
    return 0 if 失败 == 0 else 1


def _写入中间结果(
    新片段: list[pd.DataFrame],
    已有数据: pd.DataFrame | None,
    路径: Path,
) -> None:
    if not 新片段 and 已有数据 is None:
        return
    合并列表 = []
    if 已有数据 is not None:
        合并列表.append(已有数据)
    if 新片段:
        合并列表.append(pd.concat(新片段, ignore_index=True))
    if not 合并列表:
        return
    结果 = pd.concat(合并列表, ignore_index=True)
    结果 = 结果[["board_code", "board_name", "stock_code", "stock_name"]].drop_duplicates(subset=["board_code", "stock_code"])
    数据目录.mkdir(parents=True, exist_ok=True)
    原子写入Parquet(结果, 路径)
    log(f"已写入 {路径.name}：{len(结果):,} 行，{结果['board_code'].nunique()} 个板块，{结果['stock_code'].nunique()} 只股票")


if __name__ == "__main__":
    raise SystemExit(main())
