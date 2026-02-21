#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import os
import time
from collections import deque
from datetime import timedelta
from pathlib import Path


def clear_screen() -> None:
    os.system("cls" if os.name == "nt" else "clear")


def fmt_eta(seconds: float) -> str:
    if seconds <= 0:
        return "--:--:--"
    return str(timedelta(seconds=int(seconds)))


def bar(done: int, total: int, width: int = 40) -> str:
    if total <= 0:
        return "[" + ("-" * width) + "]"
    ratio = max(0.0, min(1.0, done / total))
    filled = int(width * ratio)
    return "[" + ("=" * filled) + ("-" * (width - filled)) + "]"


def read_json(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def parse_done_total(progress_json: dict) -> tuple[int, int]:
    done = 0
    total = 0
    for v in progress_json.values():
        if isinstance(v, list):
            total = max(total, len(v))
        elif isinstance(v, int):
            done = max(done, int(v))
    # Some runs may only have done count from int and no full list.
    if total < done:
        total = done
    return done, total


def tail_lines(path: Path, n: int = 20) -> list[str]:
    if not path.exists():
        return []
    try:
        lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
        return lines[-n:]
    except Exception:
        return []


def count_monthly_batches(data_dir: Path) -> int:
    if not data_dir.exists():
        return 0
    return len(list(data_dir.glob("batch_*.parquet")))


def main() -> None:
    progress_dir = Path("进度")
    data_dir = Path("数据") / "临时月线"

    log_path = progress_dir / "下载月线_实时.log"
    err_path = progress_dir / "下载月线_错误.log"
    json_path = progress_dir / "下载月线_进度.json"

    history: deque[tuple[float, int]] = deque(maxlen=180)

    while True:
        now = time.time()
        progress = read_json(json_path)
        done, total = parse_done_total(progress)
        batches = count_monthly_batches(data_dir)
        history.append((now, done))

        speed = 0.0
        if len(history) >= 2:
            t0, d0 = history[0]
            dt = now - t0
            if dt > 0:
                speed = max(0.0, (done - d0) / dt)

        remaining = max(0, total - done)
        eta = (remaining / speed) if speed > 0 else 0.0
        pct = (done / total * 100.0) if total > 0 else 0.0

        clear_screen()
        print("=" * 88)
        print("MONTHLY DOWNLOAD LIVE MONITOR")
        print("=" * 88)
        print(f"time:     {time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"progress: {done:,}/{total:,}  {pct:6.2f}%  {bar(done, total, 50)}")
        print(f"remain:   {remaining:,}")
        print(f"speed:    {speed:.2f} stocks/s")
        print(f"eta:      {fmt_eta(eta)}")
        print(f"batches:  {batches}")
        print()
        print(f"log:      {log_path}")
        print(f"error:    {err_path}")
        print(f"progress: {json_path}")
        print("-" * 88)
        print("LAST LOG LINES")
        print("-" * 88)
        for line in tail_lines(log_path, 20):
            print(line)
        err_tail = tail_lines(err_path, 6)
        if err_tail:
            print("-" * 88)
            print("LAST ERROR LINES")
            print("-" * 88)
            for line in err_tail:
                print(line)
        print()
        print("Press Ctrl+C to stop monitor.")
        time.sleep(2)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nmonitor stopped")
