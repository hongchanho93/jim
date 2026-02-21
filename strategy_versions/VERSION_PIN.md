# 策略版本固定说明

## 当前固定版本
- 固定版本名称：`老版最终版`
- 默认执行脚本：`strategy_versions/combined_strategy_最终版.py`
- 等价稳定脚本：`strategy_versions/combined_strategy_stable_legacy.py`

## 来源映射
- 原始快照：`strategy_versions/combined_strategy_legacy_v20260220.py`
- 固定日期：`2026-02-20`

## 当前规则状态
- 已纳入新增过滤：月线 `MA16` 至少走平（`当前 MA16 >= 上期 MA16`）。
- 已对齐清洗后换手率口径：优先使用日线 `turnover` 百分比值（`0.35` 表示 `0.35%`），仅在缺失/异常时回退 `volume / outstanding_share * 100`。
- 本规则已同步到：
  - `strategy_versions/combined_strategy_最终版.py`
  - `strategy_versions/combined_strategy_stable_legacy.py`

## 维护约束
- 后续策略微调请新建版本文件，不要直接覆盖 `strategy_versions/combined_strategy_最终版.py`。
- 做效果对比时，统一以“老版最终版”为基线。
