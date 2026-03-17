"""由15分钟数据派生30分钟/60分钟数据。"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Callable

import pandas as pd
import pyarrow.parquet as pq

from 配置15分钟 import 数据15分钟路径, 数据30分钟路径, 数据60分钟路径, 输出列名15分钟
from 数据清洗规则 import 清洗15分钟


Logger = Callable[[str], None]

时间映射30分钟 = {
    "09:45:00": "10:00:00",
    "10:00:00": "10:00:00",
    "10:15:00": "10:30:00",
    "10:30:00": "10:30:00",
    "10:45:00": "11:00:00",
    "11:00:00": "11:00:00",
    "11:15:00": "11:30:00",
    "11:30:00": "11:30:00",
    "13:15:00": "13:30:00",
    "13:30:00": "13:30:00",
    "13:45:00": "14:00:00",
    "14:00:00": "14:00:00",
    "14:15:00": "14:30:00",
    "14:30:00": "14:30:00",
    "14:45:00": "15:00:00",
    "15:00:00": "15:00:00",
}

时间映射60分钟 = {
    "09:45:00": "10:30:00",
    "10:00:00": "10:30:00",
    "10:15:00": "10:30:00",
    "10:30:00": "10:30:00",
    "10:45:00": "11:30:00",
    "11:00:00": "11:30:00",
    "11:15:00": "11:30:00",
    "11:30:00": "11:30:00",
    "13:15:00": "14:00:00",
    "13:30:00": "14:00:00",
    "13:45:00": "14:00:00",
    "14:00:00": "14:00:00",
    "14:15:00": "15:00:00",
    "14:30:00": "15:00:00",
    "14:45:00": "15:00:00",
    "15:00:00": "15:00:00",
}

派生配置 = {
    30: {"path": 数据30分钟路径, "time_map": 时间映射30分钟},
    60: {"path": 数据60分钟路径, "time_map": 时间映射60分钟},
}


def log(msg: str) -> None:
    print(msg, flush=True)


def _读取parquet最大日期(path: Path) -> str | None:
    if not path.exists():
        return None

    pf = pq.ParquetFile(path)
    names = pf.metadata.schema.names
    if "date" not in names:
        return None
    date_idx = names.index("date")

    max_date = None
    for i in range(pf.metadata.num_row_groups):
        stats = pf.metadata.row_group(i).column(date_idx).statistics
        if stats is None:
            continue
        if max_date is None or stats.max > max_date:
            max_date = stats.max
    return max_date


def _聚合分钟(df: pd.DataFrame, freq: int) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=输出列名15分钟)

    out = df.loc[:, 输出列名15分钟].copy()
    out["time"] = out["time"].map(派生配置[freq]["time_map"]).fillna("")
    out = out[out["time"] != ""].copy()
    return _聚合已映射分钟(out)


def _聚合已映射分钟(df: pd.DataFrame) -> pd.DataFrame:
    out = df.loc[:, 输出列名15分钟].copy()
    if out.empty:
        return pd.DataFrame(columns=输出列名15分钟)

    grouped = (
        out.groupby(["stock_code", "date", "time"], as_index=False, sort=False)
        .agg(
            stock_name=("stock_name", "last"),
            open=("open", "first"),
            high=("high", "max"),
            low=("low", "min"),
            close=("close", "last"),
            volume=("volume", "sum"),
            amount=("amount", "sum"),
        )
        .loc[:, 输出列名15分钟]
    )

    cleaned, _ = 清洗15分钟(grouped)
    return cleaned.loc[:, 输出列名15分钟]


def 聚合到30和60分钟(df: pd.DataFrame) -> dict[int, pd.DataFrame]:
    base = df.loc[:, 输出列名15分钟].copy()
    base = base.sort_values(["stock_code", "date", "time"], kind="mergesort").reset_index(drop=True)
    return {freq: _聚合分钟(base, freq) for freq in 派生配置}


def _调用低内存合并(batch_dir: Path, output: Path, work_dir: Path) -> None:
    cmd = [
        sys.executable,
        str(Path(__file__).resolve().parent / "merge_15min_lowmem.py"),
        "--batch-dir",
        str(batch_dir),
        "--output",
        str(output),
        "--work-dir",
        str(work_dir),
        "--prefix-width",
        "2",
    ]
    r = subprocess.run(cmd, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"low-memory merge failed: exit={r.returncode}")


def _硬链接或复制(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists():
        dst.unlink()
    try:
        os.link(src, dst)
    except Exception:
        shutil.copy2(src, dst)


def _写入增量目标(target_path: Path, incremental_df: pd.DataFrame, logger: Logger) -> None:
    if incremental_df.empty:
        logger(f"  [SKIP] {target_path.name} 无新增可写入")
        return

    temp_root = Path(tempfile.gettempdir()) / f"{target_path.stem}_{int(time.time() * 1000)}"
    batch_dir = temp_root / "batches"
    work_dir = temp_root / "work"
    output_file = temp_root / "merged.parquet"
    batch_dir.mkdir(parents=True, exist_ok=True)

    try:
        batch_idx = 0
        if target_path.exists():
            _硬链接或复制(target_path, batch_dir / f"batch_{batch_idx:06d}.parquet")
            batch_idx += 1

        incremental_df.to_parquet(
            batch_dir / f"batch_{batch_idx:06d}.parquet",
            engine="pyarrow",
            compression="snappy",
            index=False,
        )

        _调用低内存合并(batch_dir=batch_dir, output=output_file, work_dir=work_dir)

        target_tmp = target_path.with_suffix(target_path.suffix + ".tmp")
        if target_tmp.exists():
            target_tmp.unlink()
        shutil.move(str(output_file), str(target_tmp))
        target_tmp.replace(target_path)
    finally:
        shutil.rmtree(temp_root, ignore_errors=True)


def 增量更新派生分钟数据(df_15分钟增量: pd.DataFrame, logger: Logger = log) -> None:
    if df_15分钟增量.empty:
        logger("[SKIP] 无15分钟增量，跳过30/60分钟同步")
        return

    logger("\n[派生分钟] 由15分钟增量生成30分钟和60分钟...")
    aggregated = 聚合到30和60分钟(df_15分钟增量)
    for freq, out_df in aggregated.items():
        target_path = 派生配置[freq]["path"]
        logger(f"  {freq}分钟新增行数: {len(out_df):,}")
        _写入增量目标(target_path, out_df, logger)
        logger(f"  [OK] 已写回: {target_path}")


def _映射并拆分可聚合块(df: pd.DataFrame, freq: int, carry_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    if df.empty:
        return pd.DataFrame(columns=输出列名15分钟), pd.DataFrame(columns=输出列名15分钟)

    mapped = df.loc[:, 输出列名15分钟].copy()
    mapped["time"] = mapped["time"].map(派生配置[freq]["time_map"]).fillna("")
    mapped = mapped[mapped["time"] != ""].copy()
    if not carry_df.empty:
        mapped = pd.concat([carry_df, mapped], ignore_index=True)
    if mapped.empty:
        return pd.DataFrame(columns=输出列名15分钟), pd.DataFrame(columns=输出列名15分钟)

    last = mapped.iloc[-1]
    keep_mask = (
        (mapped["stock_code"] == last["stock_code"])
        & (mapped["date"] == last["date"])
        & (mapped["time"] == last["time"])
    )
    ready = mapped.loc[~keep_mask].copy()
    carry = mapped.loc[keep_mask].copy()
    return ready, carry


def 从15分钟文件补建派生分钟(
    freqs: list[int] | None = None,
    force_full: bool = False,
    logger: Logger = log,
) -> None:
    if freqs is None:
        freqs = [30, 60]

    source_path = 数据15分钟路径
    if not source_path.exists():
        raise FileNotFoundError(f"15分钟文件不存在: {source_path}")

    source_max_date = _读取parquet最大日期(source_path)
    if not source_max_date:
        raise RuntimeError("无法读取15分钟数据的最大日期")

    active_freqs: list[int] = []
    target_max_dates: dict[int, str | None] = {}
    for freq in freqs:
        target_path = 派生配置[freq]["path"]
        target_max = None if force_full else _读取parquet最大日期(target_path)
        target_max_dates[freq] = target_max
        if target_max == source_max_date and target_path.exists():
            logger(f"[SKIP] {freq}分钟已最新（{target_max}）")
            continue
        active_freqs.append(freq)

    if not active_freqs:
        return

    logger("=" * 70)
    logger("开始由15分钟文件补建30分钟/60分钟")
    logger("=" * 70)
    logger(f"源文件: {source_path}")
    logger(f"源文件最大日期: {source_max_date}")
    logger(f"目标周期: {active_freqs}")

    temp_root = Path(tempfile.gettempdir()) / f"derived_minute_{int(time.time())}"
    batch_dirs = {freq: temp_root / f"{freq}m_batches" for freq in active_freqs}
    work_dirs = {freq: temp_root / f"{freq}m_work" for freq in active_freqs}
    output_files = {freq: temp_root / f"{freq}m_output.parquet" for freq in active_freqs}
    carry = {freq: pd.DataFrame(columns=输出列名15分钟) for freq in active_freqs}
    batch_indexes = {freq: 0 for freq in active_freqs}

    try:
        for freq in active_freqs:
            batch_dirs[freq].mkdir(parents=True, exist_ok=True)

        pf = pq.ParquetFile(source_path)
        total_batches = 0

        for rb in pf.iter_batches(columns=输出列名15分钟, batch_size=500_000):
            total_batches += 1
            df = rb.to_pandas()
            if df.empty:
                continue

            df = df.sort_values(["stock_code", "date", "time"], kind="mergesort").reset_index(drop=True)
            for freq in active_freqs:
                start_date = None if force_full else target_max_dates[freq]
                part = df if start_date is None else df[df["date"] >= start_date].copy()
                ready, carry[freq] = _映射并拆分可聚合块(part, freq, carry[freq])
                if ready.empty:
                    continue

                aggregated = _聚合已映射分钟(ready)
                if aggregated.empty:
                    continue

                out_path = batch_dirs[freq] / f"batch_{batch_indexes[freq]:06d}.parquet"
                aggregated.to_parquet(out_path, engine="pyarrow", compression="snappy", index=False)
                batch_indexes[freq] += 1

            if total_batches % 20 == 0:
                logger(f"  已处理15分钟批次: {total_batches}")

        for freq in active_freqs:
            if not carry[freq].empty:
                aggregated = _聚合已映射分钟(carry[freq])
                if not aggregated.empty:
                    out_path = batch_dirs[freq] / f"batch_{batch_indexes[freq]:06d}.parquet"
                    aggregated.to_parquet(out_path, engine="pyarrow", compression="snappy", index=False)
                    batch_indexes[freq] += 1

        for freq in active_freqs:
            target_path = 派生配置[freq]["path"]
            merge_batch_dir = temp_root / f"{freq}m_merge_input"
            merge_batch_dir.mkdir(parents=True, exist_ok=True)
            merge_idx = 0

            if target_path.exists() and not force_full:
                _硬链接或复制(target_path, merge_batch_dir / f"batch_{merge_idx:06d}.parquet")
                merge_idx += 1

            for fp in sorted(batch_dirs[freq].glob("batch_*.parquet")):
                shutil.copy2(fp, merge_batch_dir / f"batch_{merge_idx:06d}.parquet")
                merge_idx += 1

            if merge_idx == 0:
                logger(f"[SKIP] {freq}分钟未生成任何数据")
                continue

            logger(f"\n[合并] {freq}分钟数据...")
            _调用低内存合并(
                batch_dir=merge_batch_dir,
                output=output_files[freq],
                work_dir=work_dirs[freq],
            )

            target_tmp = target_path.with_suffix(target_path.suffix + ".tmp")
            if target_tmp.exists():
                target_tmp.unlink()
            shutil.move(str(output_files[freq]), str(target_tmp))
            target_tmp.replace(target_path)
            logger(f"[OK] {freq}分钟已写回: {target_path}")

    finally:
        shutil.rmtree(temp_root, ignore_errors=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="由15分钟数据补建30分钟/60分钟数据")
    parser.add_argument(
        "--freqs",
        nargs="*",
        type=int,
        default=[30, 60],
        choices=[30, 60],
        help="需要补建的目标周期，默认 30 60",
    )
    parser.add_argument("--force-full", action="store_true", help="忽略现有30/60分钟文件，强制全量重建")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    从15分钟文件补建派生分钟(freqs=args.freqs, force_full=args.force_full, logger=log)


if __name__ == "__main__":
    main()
