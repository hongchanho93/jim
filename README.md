# Low-Point 1.5x Strategy

## What this project does
This scanner implements the plan:
- If listing date is before `2019-01-01`, use monthly `MA32`.
- Otherwise use weekly `MA64`.
- Find the latest swing event where `peak / prior_bottom >= 2.5`.
- Define new low as the absolute low from that peak to current.
- Trigger signal when `MA / new_low` is in `[1.45, 1.55]`.
- Use a causal engine to avoid look-ahead bias:
  - Fractal pivots are only usable after right-side `N` bars are observed.
  - Rally event only becomes active after the peak is confirmed.
  - New low is maintained as rolling minimum from confirmed peak to current bar.
  - Consecutive in-range bars are deduplicated into one signal.
  - For each consecutive zone, keep only the bar with ratio closest to `1.50`.
  - A new signal can appear only after ratio exits the in-range zone and re-enters.

## Main script
- `ma_new_low_ratio_scan.py`

## Input data
Parquet file with at least:
- `date`
- `code`
- `high`
- `low`
- `close`

Optional fields:
- `open` (if missing, close is used)
- `list_date` (if missing, first trading date is used as listing proxy)

Supported alias names:
- Date: `date`, `trade_date`, `datetime`, `dt`
- Code: `code`, `symbol`, `ts_code`, `ticker`
- Listing date: `list_date`, `ipo_date`, `listing_date`

## Run
```bash
python ma_new_low_ratio_scan.py --data all_data_baostock_2025.parquet
```

Common options:
- `--out outputs/ma_new_low_ratio_signals.csv`
- `--split-date 2019-01-01`
- `--pre-window 32`
- `--post-window 64`
- `--fractal-n 3`
- `--min-multiple 2.5`
- `--ratio-low 1.45`
- `--ratio-high 1.55`
- `--target-ratio 1.50`

## Output files
For `--out outputs/ma_new_low_ratio_signals.csv`, the script writes:
- `outputs/ma_new_low_ratio_signals_raw.csv`: one row per code with a valid rally event
- `outputs/ma_new_low_ratio_signals.csv`: rows where signal is true
- `outputs/ma_new_low_ratio_signals_summary.csv`: global summary
- `outputs/ma_new_low_ratio_signals_skipped_summary.csv`: skipped reason counts

Key columns for anti-look-ahead check:
- `bottom_confirm_date`
- `peak_confirm_date`
- `last_signal_date`
- `signal_count`

## Tests
```bash
pytest -q
```
