#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Low-memory merge for 15min batch parquet files.

Why:
- Original merge loads all batch files into one pandas DataFrame, which can OOM on 16GB RAM.
- This script partitions by stock_code prefix, then dedups/sorts per partition and appends to one parquet file.
"""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def log(msg: str) -> None:
    print(msg, flush=True)


def build_prefixes(width: int) -> list[str]:
    if width == 1:
        return [str(i) for i in range(10)]
    if width == 2:
        return [f"{i:02d}" for i in range(100)]
    if width == 3:
        return [f"{i:03d}" for i in range(1000)]
    if width == 4:
        return [f"{i:04d}" for i in range(10000)]
    raise ValueError("prefix width must be between 1 and 4")


def split_batches(
    batch_files: list[Path],
    shard_dir: Path,
    prefix_width: int,
    key_col: str = "stock_code",
) -> None:
    shard_dir.mkdir(parents=True, exist_ok=True)
    chunk_idx = 0
    for idx, fp in enumerate(batch_files, start=1):
        log(f"[1/3] split {idx}/{len(batch_files)}: {fp.name}")
        pf = pq.ParquetFile(fp)

        for rb in pf.iter_batches(batch_size=500_000):
            df = rb.to_pandas()
            if key_col not in df.columns:
                raise KeyError(f"missing column: {key_col} in {fp}")

            prefix = df[key_col].astype(str).str.slice(0, prefix_width)
            df = df.assign(_prefix=prefix)

            # 一个chunk会拆成多个prefix文件，避免高内存常驻
            for p, g in df.groupby("_prefix", sort=False):
                out_dir = shard_dir / p
                out_dir.mkdir(parents=True, exist_ok=True)
                out_fp = out_dir / f"{fp.stem}_c{chunk_idx:06d}.parquet"
                g.drop(columns=["_prefix"]).to_parquet(
                    out_fp, engine="pyarrow", compression="snappy", index=False
                )
            chunk_idx += 1


def merge_shards(
    shard_dir: Path,
    output_file: Path,
    prefixes: list[str],
    key_cols: list[str],
) -> int:
    output_file.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_file.with_suffix(output_file.suffix + ".tmp")
    if tmp_out.exists():
        tmp_out.unlink()

    writer: pq.ParquetWriter | None = None
    total_rows = 0
    total_removed = 0

    try:
        present_prefixes = sorted([p.name for p in shard_dir.iterdir() if p.is_dir()])
        merge_prefixes = present_prefixes if present_prefixes else prefixes

        for i, p in enumerate(merge_prefixes, start=1):
            part_dir = shard_dir / p
            files = sorted(part_dir.glob("*.parquet"))
            if not files:
                continue

            log(f"[2/3] merge shard {i}/{len(merge_prefixes)}: {p} ({len(files)} files)")
            df = pd.concat([pd.read_parquet(x) for x in files], ignore_index=True)
            before = len(df)
            df = df.drop_duplicates(subset=key_cols, keep="last")
            df = df.sort_values(key_cols).reset_index(drop=True)
            removed = before - len(df)
            total_removed += removed
            total_rows += len(df)
            if removed:
                log(f"      dedup removed in shard {p}: {removed:,}")

            table = pa.Table.from_pandas(df, preserve_index=False)
            if writer is None:
                writer = pq.ParquetWriter(tmp_out, table.schema, compression="snappy")
            writer.write_table(table)
    finally:
        if writer is not None:
            writer.close()

    if not tmp_out.exists():
        raise RuntimeError("no output generated; check shard files")
    if output_file.exists():
        output_file.unlink()
    tmp_out.replace(output_file)

    log(f"[3/3] done")
    log(f"      rows: {total_rows:,}")
    log(f"      dedup removed: {total_removed:,}")
    log(f"      output: {output_file}")
    log(f"      size: {output_file.stat().st_size / (1024 * 1024):.1f} MB")
    return total_rows


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Low-memory merge for 15min batch parquet")
    p.add_argument("--batch-dir", default="数据/临时15分钟", help="batch parquet folder")
    p.add_argument("--output", default="数据/全市场15分钟数据.parquet", help="final parquet path")
    p.add_argument(
        "--work-dir",
        default="数据/临时15分钟_低内存合并",
        help="temporary shard folder",
    )
    p.add_argument(
        "--prefix-width",
        type=int,
        default=4,
        choices=[1, 2, 3, 4],
        help="stock_code prefix width for sharding",
    )
    p.add_argument(
        "--keep-work-dir",
        action="store_true",
        help="keep temporary shard folder after completion",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    batch_dir = Path(args.batch_dir)
    output_file = Path(args.output)
    work_dir = Path(args.work_dir)

    batch_files = sorted(batch_dir.glob("batch_*.parquet"))
    if not batch_files:
        raise FileNotFoundError(f"no batch files found in: {batch_dir}")

    log("=" * 70)
    log("Low-memory merge for 15min data")
    log("=" * 70)
    log(f"batch files: {len(batch_files)}")
    log(f"batch dir:   {batch_dir}")
    log(f"output:      {output_file}")
    log(f"work dir:    {work_dir}")
    log(f"prefix width:{args.prefix_width}")
    log("=" * 70)

    if work_dir.exists():
        shutil.rmtree(work_dir)
    work_dir.mkdir(parents=True, exist_ok=True)

    split_batches(batch_files, work_dir, args.prefix_width)
    prefixes = build_prefixes(args.prefix_width)
    merge_shards(
        shard_dir=work_dir,
        output_file=output_file,
        prefixes=prefixes,
        key_cols=["stock_code", "date", "time"],
    )

    if not args.keep_work_dir:
        shutil.rmtree(work_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
