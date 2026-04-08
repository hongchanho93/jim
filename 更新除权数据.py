"""更新当前项目使用的全市场除权/股本变更缓存。"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Sequence

import pandas as pd

from 除权数据抓取 import (
    FetchStats,
    build_local_share_change_fallback,
    fetch_bulk_dividend_events,
    fetch_cninfo_dividends_for_code,
    fetch_rights_for_code,
    fetch_share_changes_for_code,
    fetch_share_changes_from_cninfo_for_code,
    fetch_ths_dividends_for_code,
    log,
    merge_actions,
    resolve_target_codes,
    run_parallel_map,
)


项目目录 = Path(__file__).resolve().parent
默认输出目录 = 项目目录 / "数据" / "除权数据"


def build_action_frame(merged_actions: dict[str, list]) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for code, actions in merged_actions.items():
        for action in actions:
            rows.append(
                {
                    "stock_code": code,
                    "ex_date": action.ex_date,
                    "bonus_per10": action.bonus_per10,
                    "transfer_per10": action.transfer_per10,
                    "cash_div_per10": action.cash_div_per10,
                    "rights_per10": action.rights_per10,
                    "rights_price": action.rights_price,
                }
            )
    frame = pd.DataFrame(
        rows,
        columns=[
            "stock_code",
            "ex_date",
            "bonus_per10",
            "transfer_per10",
            "cash_div_per10",
            "rights_per10",
            "rights_price",
        ],
    )
    if frame.empty:
        return frame
    return frame.sort_values(["stock_code", "ex_date"], ascending=[True, False], ignore_index=True)


def build_share_frame(share_changes: dict[str, list]) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for code, items in share_changes.items():
        for item in items:
            rows.append(
                {
                    "stock_code": code,
                    "change_date": item.change_date,
                    "float_shares": item.float_shares,
                }
            )
    frame = pd.DataFrame(rows, columns=["stock_code", "change_date", "float_shares"])
    if frame.empty:
        return frame
    return frame.sort_values(["stock_code", "change_date"], ascending=[True, False], ignore_index=True)


def write_summary(path: Path, stats: FetchStats, args: argparse.Namespace, elapsed: float, subset_mode: bool) -> None:
    summary = {
        "generated_at": pd.Timestamp.now(tz="Asia/Seoul").isoformat(),
        "subset_mode": subset_mode,
        "codes": stats.total_codes,
        "dividend_codes_from_bulk": stats.dividend_codes_from_bulk,
        "dividend_codes_from_cninfo": stats.dividend_codes_from_cninfo,
        "dividend_codes_from_ths": stats.dividend_codes_from_ths,
        "rights_codes": stats.rights_codes,
        "share_change_codes_from_em": stats.share_change_codes_from_em,
        "share_change_codes_from_cninfo": stats.share_change_codes_from_cninfo,
        "share_change_codes": stats.share_change_codes,
        "share_change_codes_from_local_fallback": stats.share_change_codes_from_local_fallback,
        "codes_without_dividends": stats.codes_without_dividends,
        "codes_without_share_changes": stats.codes_without_share_changes,
        "total_dividend_events": stats.total_dividend_events,
        "total_rights_events": stats.total_rights_events,
        "total_share_change_points": stats.total_share_change_points,
        "elapsed_seconds": round(elapsed, 1),
        "arguments": {
            "codes": args.codes,
            "limit": args.limit,
            "workers": args.workers,
            "skip_rights": args.skip_rights,
            "skip_share_changes": args.skip_share_changes,
            "output_dir": str(args.output_dir),
        },
    }
    path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    log(f"[write] {path}")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="更新当前项目使用的全市场除权/股本变更缓存。")
    parser.add_argument("--codes", type=str, default=None, help="逗号或空格分隔的股票代码，仅更新指定股票。")
    parser.add_argument("--limit", type=int, default=None, help="调试：仅处理前 N 只股票。")
    parser.add_argument("--workers", type=int, default=12, help="并发抓取 worker 数。")
    parser.add_argument("--skip-rights", action="store_true", help="调试：跳过配股抓取。")
    parser.add_argument("--skip-share-changes", action="store_true", help="调试：跳过股本变更抓取。")
    parser.add_argument("--output-dir", type=Path, default=默认输出目录, help="输出目录，默认写入 数据/除权数据。")
    args = parser.parse_args(argv)

    start_time = time.time()
    codes = resolve_target_codes(selected=args.codes, limit=args.limit)
    subset_mode = args.codes is not None
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    stats = FetchStats(total_codes=len(codes))
    log(f"[universe] loaded {len(codes)} codes")
    if subset_mode:
        log(f"[universe] subset={','.join(codes)}")

    bulk_dividends, bulk_codes = fetch_bulk_dividend_events(codes)
    stats.dividend_codes_from_bulk = len(set(codes) & bulk_codes)

    missing_dividend_codes = [code for code in codes if not bulk_dividends.get(code)]
    log(f"[dividend] fallback补数 for {len(missing_dividend_codes)} bulk-missing codes")

    cninfo_dividends: dict[str, list] = {}
    ths_dividends: dict[str, list] = {}
    if missing_dividend_codes:
        dividend_workers = min(args.workers, 2)
        cninfo_dividends = run_parallel_map(
            missing_dividend_codes,
            fetch_cninfo_dividends_for_code,
            dividend_workers,
            "cninfo-dividend",
            backend="process",
        )
        stats.dividend_codes_from_cninfo = sum(1 for actions in cninfo_dividends.values() if actions)
        still_missing = [code for code in missing_dividend_codes if not cninfo_dividends.get(code)]
        if still_missing:
            ths_dividends = run_parallel_map(
                still_missing,
                fetch_ths_dividends_for_code,
                dividend_workers,
                "ths-dividend",
                backend="process",
            )
            stats.dividend_codes_from_ths = sum(1 for actions in ths_dividends.values() if actions)

    rights_actions: dict[str, list] = {}
    if args.skip_rights:
        log("[rights] skipped by flag")
    else:
        rights_actions = run_parallel_map(codes, fetch_rights_for_code, args.workers, "rights", backend="process")
        stats.rights_codes = sum(1 for actions in rights_actions.values() if actions)

    share_changes: dict[str, list] = {}
    if args.skip_share_changes:
        log("[share-change] skipped by flag")
    else:
        share_change_workers = min(args.workers, 6)
        log(f"[share-change] using {share_change_workers} workers")
        share_changes = run_parallel_map(codes, fetch_share_changes_for_code, share_change_workers, "share-change", backend="process")
        stats.share_change_codes_from_em = sum(1 for items in share_changes.values() if items)
        missing_share_codes = [code for code in codes if not share_changes.get(code)]
        if missing_share_codes:
            cninfo_share_changes = run_parallel_map(
                missing_share_codes,
                fetch_share_changes_from_cninfo_for_code,
                min(args.workers, 2),
                "share-change-cninfo",
                backend="process",
            )
            for code, items in cninfo_share_changes.items():
                if items:
                    share_changes[code] = items
            stats.share_change_codes_from_cninfo = sum(1 for items in cninfo_share_changes.values() if items)
            missing_share_codes = [code for code in codes if not share_changes.get(code)]
        if missing_share_codes:
            local_fallback = build_local_share_change_fallback(missing_share_codes)
            for code, items in local_fallback.items():
                if items:
                    share_changes[code] = items
            stats.share_change_codes_from_local_fallback = sum(1 for code in missing_share_codes if share_changes.get(code))
        stats.share_change_codes = sum(1 for items in share_changes.values() if items)

    merged_actions = merge_actions(codes, bulk_dividends, cninfo_dividends, ths_dividends, rights_actions)
    stats.total_dividend_events = sum(
        1
        for actions in merged_actions.values()
        for action in actions
        if action.bonus_per10 or action.transfer_per10 or action.cash_div_per10
    )
    stats.total_rights_events = sum(1 for actions in merged_actions.values() for action in actions if action.rights_per10)
    stats.codes_without_dividends = sum(1 for code in codes if not merged_actions.get(code))
    stats.total_share_change_points = sum(len(items) for items in share_changes.values())
    stats.codes_without_share_changes = sum(1 for code in codes if not share_changes.get(code))

    corporate_actions_path = output_dir / "corporate_actions.parquet"
    share_changes_path = output_dir / "share_changes.parquet"
    summary_path = output_dir / "fetch_summary.json"

    build_action_frame(merged_actions).to_parquet(corporate_actions_path, index=False)
    log(f"[write] {corporate_actions_path}")
    build_share_frame(share_changes).to_parquet(share_changes_path, index=False)
    log(f"[write] {share_changes_path}")

    elapsed = time.time() - start_time
    write_summary(summary_path, stats, args, elapsed, subset_mode)

    log("[summary]")
    log(f"  codes={stats.total_codes}")
    log(f"  total_dividend_events={stats.total_dividend_events}")
    log(f"  total_rights_events={stats.total_rights_events}")
    log(f"  total_share_change_points={stats.total_share_change_points}")
    log(f"  corporate_actions_output={corporate_actions_path.name}")
    log(f"  share_changes_output={share_changes_path.name}")
    log(f"  elapsed_seconds={elapsed:.1f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
