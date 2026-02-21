"""实时显示15分钟下载进度看板。"""

from __future__ import annotations

import json
import os
import time
from collections import deque
from datetime import timedelta
from pathlib import Path

import pandas as pd
from 配置15分钟 import 临时文件目录, 开始日期, 批量大小, 进度文件路径


def _fmt_eta(seconds: float) -> str:
    if seconds <= 0:
        return "00:00:00"
    return str(timedelta(seconds=int(seconds)))


def _bar(done: int, total: int, width: int = 50) -> str:
    if total <= 0:
        return "[" + "-" * width + "]"
    ratio = max(0.0, min(1.0, done / total))
    filled = int(width * ratio)
    return "[" + "=" * filled + "-" * (width - filled) + "]"


def _detect_total_stocks() -> int:
    data_dir = Path("数据")
    if not data_dir.exists():
        return 0
    daily = None
    for p in data_dir.glob("*.parquet"):
        if "历史" in p.name:
            daily = p
            break
    if daily is None:
        files = sorted(data_dir.glob("*.parquet"), key=lambda x: x.stat().st_size)
        if not files:
            return 0
        daily = files[0]
    df = pd.read_parquet(daily, columns=["stock_code"])
    return int(df["stock_code"].nunique())


def _load_progress(progress_path: Path) -> int:
    if not progress_path.exists():
        return 0
    try:
        with open(progress_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return 0

    done = 0
    for value in data.values():
        if isinstance(value, list):
            done = max(done, len(value))
        elif isinstance(value, int):
            done = max(done, value)
    return done


def _count_batch_files(temp_dir: Path) -> int:
    if not temp_dir.exists():
        return 0
    return len(list(temp_dir.glob("batch_*.parquet")))


def main() -> None:
    total = _detect_total_stocks()
    total_batches = (total + 批量大小 - 1) // 批量大小 if total > 0 else 0
    history: deque[tuple[float, int]] = deque(maxlen=120)

    try:
        while True:
            now = time.time()
            done = _load_progress(进度文件路径)
            batches = _count_batch_files(临时文件目录)
            history.append((now, done))

            speed = 0.0
            if len(history) >= 2:
                t0, d0 = history[0]
                dt = now - t0
                if dt > 0:
                    speed = max(0.0, (done - d0) / dt)

            remaining = max(0, total - done)
            eta = (remaining / speed) if speed > 0 else 0
            pct = (done / total * 100.0) if total > 0 else 0.0

            os.system("cls" if os.name == "nt" else "clear")
            print("=" * 76)
            print("15分钟下载实时看板")
            print("=" * 76)
            print(f"时间: {time.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"下载范围: {开始日期} ~ today")
            print()

            print("股票进度")
            print(f"  {_bar(done, total, 60)} {pct:6.2f}%")
            print(f"  已完成: {done:,} / {total:,}")
            print(f"  剩余:   {remaining:,}")
            print()

            batch_pct = (batches / total_batches * 100.0) if total_batches > 0 else 0.0
            print("批次进度")
            print(f"  {_bar(batches, total_batches, 60)} {batch_pct:6.2f}%")
            print(f"  批次文件: {batches:,} / {total_batches:,} (batch size={批量大小})")
            print()

            print("速度与预计")
            print(f"  当前速度: {speed:.2f} 只/秒")
            print(f"  预计剩余: {_fmt_eta(eta)}")
            print()

            print(f"进度文件: {进度文件路径}")
            print(f"临时目录: {临时文件目录}")
            print()
            print("按 Ctrl+C 退出看板（不会影响下载任务）")

            time.sleep(2)
    except KeyboardInterrupt:
        print("\n看板已退出。")


if __name__ == "__main__":
    main()
