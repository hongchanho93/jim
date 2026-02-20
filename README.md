# 低点 1.5 倍 + MA750 联合策略

## 版本说明
- 本项目的固定最终版脚本为 `strategy_versions/combined_strategy_最终版.py`。
- 同步保留英文命名版本 `strategy_versions/combined_strategy_stable_legacy.py`（内容一致）。
- 版本固定说明见 `strategy_versions/VERSION_PIN.md`。

## 策略概览（最终版口径）
最终信号需要同时满足以下条件。

1. 第一阶段（低点 1.5 倍信号）
- 上市日早于 `2019-01-01` 使用月线 `MA32`；否则使用周线 `MA64`。
- 使用分型参数 `n=3` 寻找“底 -> 顶”波段。
- 该波段涨幅倍数必须 `>= 2.5`。
- 顶点之后滚动记录新低，计算 `MA / 新低价`。
- 比值落在 `[1.45, 1.55]` 视为命中区间，连续命中区间只保留最接近 `1.50` 的时点。

2. 第二阶段（15m MA750 共振 + 多头）
- 15m 数据构造 `MA750(15m/30m/60m)`。
- 共振条件：`(max(MA)-min(MA))/min(MA) <= 0.03`。
- 多头条件：`MA15 > MA30 > MA60`（严格大于）。
- 共振后 `3` 个自然月内必须出现多头时点。

3. 两阶段匹配
- 第一阶段日期与第二阶段日期需在 `±2` 个自然月内匹配。
- 多个候选时取日期差最小者。

4. 换手率过滤
- 以信号月为基准，向前回看 `18` 个月。
- 这 `18` 个月里至少有 1 个月月换手率和 `> 1.0`。

5. 新增过滤（2026-02-20 后）
- 月线 `MA16` 必须“至少走平”：`当前 MA16 >= 上期 MA16`。

6. 可评估性要求
- 信号后需有至少 `60` 个交易日数据（用于计算 `ret20/ret40/ret60`），否则剔除。

## 关键参数（最终版常量）
- `FRACTAL_N = 3`
- `MIN_MULTIPLE = 2.5`
- `RATIO_LOW = 1.45`
- `RATIO_HIGH = 1.55`
- `TARGET_RATIO = 1.50`
- `PRE_WINDOW = 32`（月线）
- `POST_WINDOW = 64`（周线）
- `MA_WINDOW = 750`
- `RESONANCE_TOL = 0.03`
- `POST_RESONANCE_MONTHS = 3`
- `MATCH_WINDOW_MONTHS = 2`
- `TURNOVER_LOOKBACK_MONTHS = 18`
- `MONTH_TURNOVER_MIN = 1.0`
- `MONTHLY_MA_WINDOW = 16`

## 数据要求
脚本会在项目上级目录自动搜索 Parquet 文件：
- 15m 数据需包含列：`stock_code, stock_name, date, time, open, high, low, close`
- 日线数据需包含列：`stock_code, stock_name, date, open, high, low, close, turnover`

## 运行方式
在项目根目录执行：

```bash
python strategy_versions/combined_strategy_最终版.py
```

## 输出文件
默认输出到 `outputs/`：
- `signal_threshold_lists_2025.xlsx`
- `signal_code_name_date_2025.xlsx`
- `stocks_signal_time_2025.csv`
- `signal_post_window_detail_2025.csv`
- `signal_post_window_hit_ratio_2025.csv`

## 备注
- `combined_1p5_ma750_backtest.py` 是新口径可配置版本，不作为本项目固定最终版基线。
- 若要改规则，请新建版本文件，不直接覆盖最终版。
