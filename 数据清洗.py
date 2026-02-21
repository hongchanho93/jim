"""三周期数据一次性清洗脚本。

默认按：日线 -> 月线 -> 15分钟 顺序执行。
支持 dry-run（只输出统计，不覆盖原文件）。
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

from 配置 import 合并数据路径
from 配置月线 import 数据月线路径
from 配置15分钟 import 数据15分钟路径
from 数据清洗规则 import (
    原子写入Parquet,
    打印报告,
    汇总15分钟文件,
    清洗15分钟分块,
    清洗日线,
    清洗月线,
)


def _run_lowmem_merge(batch_dir: Path, output_file: Path, work_dir: Path) -> None:
    cmd = [
        sys.executable,
        str(Path(__file__).resolve().parent / "merge_15min_lowmem.py"),
        "--batch-dir",
        str(batch_dir),
        "--output",
        str(output_file),
        "--work-dir",
        str(work_dir),
        "--prefix-width",
        "2",
    ]
    r = subprocess.run(cmd, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"low-memory merge failed: exit={r.returncode}")


def _clean_daily(dry_run: bool) -> None:
    print("\n" + "#" * 88)
    print("[日线] 开始清洗")
    print("#" * 88)

    if not 合并数据路径.exists():
        print(f"[跳过] 日线文件不存在: {合并数据路径}")
        return

    df = pd.read_parquet(合并数据路径)
    cleaned, report = 清洗日线(df)
    打印报告("日线清洗报告", report)

    if dry_run:
        print(f"[DRY-RUN] 不写入文件: {合并数据路径}")
        return

    原子写入Parquet(cleaned, 合并数据路径)
    print(f"[OK] 已写回: {合并数据路径}")


def _clean_monthly(dry_run: bool) -> None:
    print("\n" + "#" * 88)
    print("[月线] 开始清洗")
    print("#" * 88)

    if not 数据月线路径.exists():
        print(f"[跳过] 月线文件不存在: {数据月线路径}")
        return

    df = pd.read_parquet(数据月线路径)
    cleaned, report = 清洗月线(df)
    打印报告("月线清洗报告", report)

    if dry_run:
        print(f"[DRY-RUN] 不写入文件: {数据月线路径}")
        return

    原子写入Parquet(cleaned, 数据月线路径)
    print(f"[OK] 已写回: {数据月线路径}")


def _clean_15m(dry_run: bool, batch_size: int) -> None:
    print("\n" + "#" * 88)
    print("[15分钟] 开始清洗（分块+低内存合并）")
    print("#" * 88)

    if not 数据15分钟路径.exists():
        print(f"[跳过] 15分钟文件不存在: {数据15分钟路径}")
        return

    before = 汇总15分钟文件(数据15分钟路径, batch_size=batch_size)
    print("\n[15分钟 Before]")
    for k in sorted(before.keys()):
        print(f"  {k}: {before[k]}")

    root = Path(tempfile.gettempdir()) / f"clean_15m_{int(time.time())}"
    batch_dir = root / "batches"
    work_dir = root / "work"
    out_file = root / "cleaned_15m.parquet"
    batch_dir.mkdir(parents=True, exist_ok=True)

    try:
        pf = pq.ParquetFile(数据15分钟路径)
        idx = 0
        total_in = 0
        total_out = 0

        for batch in pf.iter_batches(batch_size=batch_size):
            df = batch.to_pandas()
            total_in += len(df)

            cleaned = 清洗15分钟分块(df)
            total_out += len(cleaned)

            fp = batch_dir / f"batch_{idx:06d}.parquet"
            cleaned.to_parquet(fp, engine="pyarrow", compression="snappy", index=False)
            idx += 1

            if idx % 20 == 0:
                print(f"  [chunk] {idx} chunks processed, input_rows={total_in:,}, cleaned_rows={total_out:,}")

        print(f"\n  chunks: {idx}")
        print(f"  input rows: {total_in:,}")
        print(f"  cleaned rows(before global dedup): {total_out:,}")

        _run_lowmem_merge(batch_dir=batch_dir, output_file=out_file, work_dir=work_dir)

        after = 汇总15分钟文件(out_file, batch_size=batch_size)
        print("\n[15分钟 After]")
        for k in sorted(after.keys()):
            print(f"  {k}: {after[k]}")

        if dry_run:
            print(f"[DRY-RUN] 不覆盖原文件: {数据15分钟路径}")
            return

        target_tmp = 数据15分钟路径.with_suffix(数据15分钟路径.suffix + ".tmp")
        if target_tmp.exists():
            target_tmp.unlink()
        shutil.move(str(out_file), str(target_tmp))
        target_tmp.replace(数据15分钟路径)
        print(f"[OK] 已写回: {数据15分钟路径}")

    finally:
        shutil.rmtree(root, ignore_errors=True)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="清洗日线/月线/15分钟数据")
    p.add_argument("--dry-run", action="store_true", help="仅统计与模拟，不覆盖原文件")
    p.add_argument(
        "--only",
        choices=["all", "daily", "monthly", "15m"],
        default="all",
        help="只执行指定数据类型",
    )
    p.add_argument("--batch-size", type=int, default=600_000, help="15分钟分块大小")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    print("=" * 88)
    print("三周期数据清洗")
    print("=" * 88)
    print(f"dry_run: {args.dry_run}")
    print(f"only: {args.only}")

    if args.only in ("all", "daily"):
        _clean_daily(dry_run=args.dry_run)

    if args.only in ("all", "monthly"):
        _clean_monthly(dry_run=args.dry_run)

    if args.only in ("all", "15m"):
        _clean_15m(dry_run=args.dry_run, batch_size=max(100_000, args.batch_size))

    print("\n" + "=" * 88)
    print("清洗流程结束")
    print("=" * 88)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[INTERRUPTED] 用户中断")
        sys.exit(1)
