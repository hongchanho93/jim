#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
探索板块/行业数据源
调查 BaoStock 和 AkShare 的行业分类与板块历史行情数据
"""

import traceback

def section(title):
    print("\n" + "="*70)
    print(f"  {title}")
    print("="*70)

def show_df(df, label=""):
    if df is None:
        print(f"  [{label}] DataFrame is None")
        return
    print(f"\n  [{label}]")
    print(f"  shape   : {df.shape}")
    print(f"  columns : {df.columns.tolist()}")
    print(f"  dtypes  :\n{df.dtypes.to_string()}")
    print(f"  head(5) :\n{df.head(5).to_string()}")

# ─────────────────────────────────────────────────────────────────────────────
# 1. BaoStock 行业分类
# ─────────────────────────────────────────────────────────────────────────────
section("1. BaoStock 行业分类")

try:
    import baostock as bs

    print("\n--- 登录 BaoStock ---")
    lg = bs.login()
    print(f"  login error_code: {lg.error_code}, error_msg: {lg.error_msg}")

    # 1a. 查询全市场行业分类
    print("\n--- 1a. query_stock_industry() 全市场 ---")
    try:
        rs = bs.query_stock_industry()
        print(f"  error_code: {rs.error_code}, error_msg: {rs.error_msg}")
        print(f"  fields: {rs.fields}")
        rows = []
        while rs.error_code == '0':
            row = rs.get_row_data()
            if not row:
                break
            rows.append(row)
        import pandas as pd
        df_industry = pd.DataFrame(rows, columns=rs.fields)
        show_df(df_industry, "全市场行业分类")
        print(f"\n  行业种类（industry列唯一值）:")
        if 'industry' in df_industry.columns:
            vals = df_industry['industry'].unique()
            print(f"  共 {len(vals)} 个行业，前20个: {vals[:20].tolist()}")
    except Exception as e:
        print(f"  [ERROR] query_stock_industry() 全市场: {e}")
        traceback.print_exc()

    # 1b. 查询单只股票行业
    print("\n--- 1b. query_stock_industry(code='sh.600000') ---")
    try:
        rs2 = bs.query_stock_industry(code="sh.600000")
        print(f"  error_code: {rs2.error_code}, error_msg: {rs2.error_msg}")
        print(f"  fields: {rs2.fields}")
        rows2 = []
        while rs2.error_code == '0':
            row = rs2.get_row_data()
            if not row:
                break
            rows2.append(row)
        df2 = pd.DataFrame(rows2, columns=rs2.fields)
        show_df(df2, "sh.600000 行业")
    except Exception as e:
        print(f"  [ERROR] query_stock_industry(code='sh.600000'): {e}")
        traceback.print_exc()

    # 1c. 再试几只
    for code in ["sz.000001", "sh.601318", "sz.300750"]:
        try:
            rs3 = bs.query_stock_industry(code=code)
            rows3 = []
            while rs3.error_code == '0':
                row = rs3.get_row_data()
                if not row:
                    break
                rows3.append(row)
            df3 = pd.DataFrame(rows3, columns=rs3.fields)
            print(f"\n  {code}: {df3.to_string(index=False)}")
        except Exception as e:
            print(f"  [ERROR] {code}: {e}")

    bs.logout()
    print("\n  BaoStock 已登出")

except Exception as e:
    print(f"[ERROR] BaoStock 整体失败: {e}")
    traceback.print_exc()


# ─────────────────────────────────────────────────────────────────────────────
# 2. AkShare 行业/板块函数
# ─────────────────────────────────────────────────────────────────────────────
section("2. AkShare 行业/板块函数")

try:
    import akshare as ak
    print(f"  AkShare 版本: {ak.__version__}")
except Exception as e:
    print(f"[ERROR] 无法导入 AkShare: {e}")
    ak = None

if ak is not None:
    import pandas as pd

    # 2a. 东财行业板块列表
    section("2a. 东财行业板块列表 stock_board_industry_name_em()")
    df_industry_em = None
    try:
        df_industry_em = ak.stock_board_industry_name_em()
        show_df(df_industry_em, "东财行业板块列表")
    except Exception as e:
        print(f"  [ERROR]: {e}")
        traceback.print_exc()

    # 2b. 东财行业板块成分股
    section("2b. 东财行业板块成分股 stock_board_industry_cons_em()")
    try:
        if df_industry_em is not None and len(df_industry_em) > 0:
            # 取第一个行业名
            first_industry = df_industry_em.iloc[0, 0] if df_industry_em.shape[1] > 0 else "银行"
            # 尝试找"银行"
            if '板块名称' in df_industry_em.columns:
                names = df_industry_em['板块名称'].tolist()
                first_industry = next((n for n in names if '银行' in n), names[0] if names else '银行')
            elif df_industry_em.shape[1] > 0:
                first_industry = df_industry_em.iloc[0, 0]
            print(f"  选择行业: {first_industry}")
            df_cons = ak.stock_board_industry_cons_em(symbol=first_industry)
            show_df(df_cons, f"东财行业成分股({first_industry})")
        else:
            print("  无法获取行业列表，跳过成分股查询")
    except Exception as e:
        print(f"  [ERROR]: {e}")
        traceback.print_exc()

    # 2c. 东财概念板块列表
    section("2c. 东财概念板块列表 stock_board_concept_name_em()")
    df_concept_em = None
    try:
        df_concept_em = ak.stock_board_concept_name_em()
        show_df(df_concept_em, "东财概念板块列表")
    except Exception as e:
        print(f"  [ERROR]: {e}")
        traceback.print_exc()

    # 2d. 申万行业分类
    section("2d. 申万行业分类")
    for func_name in [
        "stock_industry_category_cninfo",
        "sw_index_first_info",
        "index_classify",
    ]:
        try:
            func = getattr(ak, func_name, None)
            if func is None:
                print(f"  ak.{func_name} 不存在，跳过")
                continue
            print(f"\n  尝试 ak.{func_name}()...")
            df = func()
            show_df(df, func_name)
        except TypeError:
            # 可能需要参数
            try:
                df = func(symbol="SW")
                show_df(df, f"{func_name}(symbol='SW')")
            except Exception as e2:
                print(f"  [ERROR] {func_name}(symbol='SW'): {e2}")
        except Exception as e:
            print(f"  [ERROR] {func_name}: {e}")
            traceback.print_exc()

    # 2e. CNINFO行业分类
    section("2e. CNINFO行业分类 stock_industry_category_cninfo()")
    for param in [None, "巨潮行业分类标准", "证监会行业分类标准", "国证行业分类标准"]:
        try:
            if param is None:
                df = ak.stock_industry_category_cninfo()
            else:
                df = ak.stock_industry_category_cninfo(symbol=param)
            show_df(df, f"cninfo({param})")
            break  # 成功则不再尝试
        except Exception as e:
            print(f"  [ERROR] stock_industry_category_cninfo({param}): {e}")

    # 2f. 申万行业成分股
    section("2f. 申万行业成分股/指数")
    for func_name in ["sw_index_first_info", "index_stock_info", "sw_index_cons"]:
        try:
            func = getattr(ak, func_name, None)
            if func is None:
                print(f"  ak.{func_name} 不存在，跳过")
                continue
            print(f"\n  尝试 ak.{func_name}()...")
            df = func()
            show_df(df, func_name)
        except Exception as e:
            print(f"  [ERROR] {func_name}: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# 3. 板块历史行情（日线/分钟线）
# ─────────────────────────────────────────────────────────────────────────────
section("3. 板块历史行情数据")

if ak is not None:
    # 3a. 东财行业板块日线历史
    section("3a. stock_board_industry_hist_em() - 日线")
    try:
        df_hist = ak.stock_board_industry_hist_em(
            symbol="银行",
            period="daily",
            start_date="20250101",
            end_date="20260331",
            adjust=""
        )
        show_df(df_hist, "银行行业板块日线历史")
    except Exception as e:
        print(f"  [ERROR] stock_board_industry_hist_em(daily): {e}")
        traceback.print_exc()

    # 3b. 月线
    section("3b. stock_board_industry_hist_em() - 月线")
    try:
        df_hist_m = ak.stock_board_industry_hist_em(
            symbol="银行",
            period="monthly",
            start_date="20200101",
            end_date="20260331",
            adjust=""
        )
        show_df(df_hist_m, "银行行业板块月线历史")
    except Exception as e:
        print(f"  [ERROR] stock_board_industry_hist_em(monthly): {e}")
        traceback.print_exc()

    # 3c. 周线
    section("3c. stock_board_industry_hist_em() - 周线")
    try:
        df_hist_w = ak.stock_board_industry_hist_em(
            symbol="银行",
            period="weekly",
            start_date="20240101",
            end_date="20260331",
            adjust=""
        )
        show_df(df_hist_w, "银行行业板块周线历史")
    except Exception as e:
        print(f"  [ERROR] stock_board_industry_hist_em(weekly): {e}")
        traceback.print_exc()

    # 3d. 概念板块历史行情
    section("3d. stock_board_concept_hist_em() - 概念板块日线")
    try:
        df_con_hist = ak.stock_board_concept_hist_em(
            symbol="ChatGPT",
            period="daily",
            start_date="20250101",
            end_date="20260331",
            adjust=""
        )
        show_df(df_con_hist, "ChatGPT概念板块日线历史")
    except Exception as e:
        print(f"  [ERROR] stock_board_concept_hist_em: {e}")
        traceback.print_exc()

    # 3e. 检查是否有分钟线
    section("3e. 板块分钟线检查")
    for func_name in [
        "stock_board_industry_hist_min_em",
        "stock_board_concept_hist_min_em",
        "stock_board_industry_minute_em",
    ]:
        func = getattr(ak, func_name, None)
        if func is None:
            print(f"  ak.{func_name} 不存在")
        else:
            print(f"  ak.{func_name} 存在，尝试调用...")
            try:
                df = func(symbol="银行")
                show_df(df, func_name)
            except Exception as e:
                print(f"  [ERROR] {func_name}: {e}")

    # 3f. 查找所有包含 "board" 的 AkShare 函数
    section("3f. AkShare 中所有 board 相关函数")
    board_funcs = [name for name in dir(ak) if 'board' in name.lower()]
    print(f"  共 {len(board_funcs)} 个 board 相关函数:")
    for f in sorted(board_funcs):
        print(f"    ak.{f}")

    # 3g. 查找所有包含 "industry" 的 AkShare 函数
    section("3g. AkShare 中所有 industry 相关函数")
    ind_funcs = [name for name in dir(ak) if 'industry' in name.lower()]
    print(f"  共 {len(ind_funcs)} 个 industry 相关函数:")
    for f in sorted(ind_funcs):
        print(f"    ak.{f}")

    # 3h. 查找申万相关
    section("3h. AkShare 中所有 sw 相关函数")
    sw_funcs = [name for name in dir(ak) if name.startswith('sw_') or 'sw_' in name]
    print(f"  共 {len(sw_funcs)} 个 sw 相关函数:")
    for f in sorted(sw_funcs):
        print(f"    ak.{f}")

    # 3i. 尝试几个申万指数函数
    section("3i. 申万指数历史数据")
    for func_name, kwargs in [
        ("sw_index_daily_indicator", {"symbol": "801010", "period": "day", "start_date": "20250101", "end_date": "20260331"}),
        ("index_zh_a_hist", {"symbol": "801010", "period": "daily", "start_date": "20250101", "end_date": "20260331"}),
    ]:
        func = getattr(ak, func_name, None)
        if func is None:
            print(f"  ak.{func_name} 不存在，跳过")
            continue
        try:
            print(f"\n  尝试 ak.{func_name}({kwargs})...")
            df = func(**kwargs)
            show_df(df, func_name)
        except Exception as e:
            print(f"  [ERROR] {func_name}: {e}")
            traceback.print_exc()

section("探索完成")
print("  所有数据源探索完毕。")
