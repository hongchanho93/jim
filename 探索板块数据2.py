import akshare as ak

# 1. 行业板块列表
print("=== 东财行业板块 ===")
try:
    df_ind = ak.stock_board_industry_name_em()
    print(f"行业板块数量: {len(df_ind)}")
    print(f"字段: {df_ind.columns.tolist()}")
    print(df_ind.head(10).to_string())
except Exception as e:
    print(f"失败: {e}")

# 2. 概念板块列表
print("\n=== 东财概念板块 ===")
try:
    df_con = ak.stock_board_concept_name_em()
    print(f"概念板块数量: {len(df_con)}")
    print(f"字段: {df_con.columns.tolist()}")
    print(df_con.head(10).to_string())
except Exception as e:
    print(f"失败: {e}")

# 3. 测试行业日线历史数据（取第一个行业）
print("\n=== 行业日线历史数据示例 ===")
try:
    df_ind2 = ak.stock_board_industry_name_em()
    first_industry = df_ind2.iloc[0]['板块名称'] if '板块名称' in df_ind2.columns else df_ind2.iloc[0, 0]
    print(f"测试行业: {first_industry}")
    df_hist = ak.stock_board_industry_hist_em(symbol=first_industry, period="daily", start_date="20250101", end_date="20260331", adjust="")
    print(f"字段: {df_hist.columns.tolist()}")
    print(f"行数: {len(df_hist)}")
    print(df_hist.head(5).to_string())
except Exception as e:
    print(f"失败: {e}")

# 4. 测试概念板块日线历史数据
print("\n=== 概念板块日线历史数据示例 ===")
try:
    df_con2 = ak.stock_board_concept_name_em()
    first_concept = df_con2.iloc[0]['板块名称'] if '板块名称' in df_con2.columns else df_con2.iloc[0, 0]
    print(f"测试概念: {first_concept}")
    df_hist2 = ak.stock_board_concept_hist_em(symbol=first_concept, period="daily", start_date="20250101", end_date="20260331", adjust="")
    print(f"字段: {df_hist2.columns.tolist()}")
    print(f"行数: {len(df_hist2)}")
    print(df_hist2.head(5).to_string())
except Exception as e:
    print(f"失败: {e}")
