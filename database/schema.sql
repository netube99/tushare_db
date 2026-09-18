-- 交易日历（时间锚点）
CREATE TABLE IF NOT EXISTS trade_cal (
    exchange       VARCHAR NOT NULL,
    cal_date       VARCHAR NOT NULL,
    is_open        BIGINT NOT NULL,
    pretrade_date  VARCHAR,
    PRIMARY KEY (exchange, cal_date)
);

-- 拉取日志（驱动回填判断）
CREATE TABLE IF NOT EXISTS pull_log (
    table_name  VARCHAR NOT NULL,
    date_val    VARCHAR NOT NULL,
    ok          BIGINT NOT NULL,
    retry_count BIGINT NOT NULL DEFAULT 0,
    last_try    VARCHAR DEFAULT NULL,
    PRIMARY KEY (table_name, date_val)
);

-- ETF基本信息
CREATE TABLE IF NOT EXISTS "etf_basic" (
    ts_code VARCHAR,
    csname VARCHAR,
    extname VARCHAR,
    cname VARCHAR,
    index_code VARCHAR,
    index_name VARCHAR,
    setup_date VARCHAR,
    list_date VARCHAR,
    list_status VARCHAR,
    exchange VARCHAR,
    mgr_name VARCHAR,
    custod_name VARCHAR,
    mgt_fee DOUBLE,
    etf_type VARCHAR,
    PRIMARY KEY (ts_code)
);

-- ETF复权因子
CREATE TABLE IF NOT EXISTS "fund_adj" (
    ts_code VARCHAR,
    trade_date VARCHAR,
    adj_factor DOUBLE,
    PRIMARY KEY (ts_code, trade_date)
);

-- 可转债技术面因子(专业版)
CREATE TABLE IF NOT EXISTS "cb_factor_pro" (
    ts_code VARCHAR,
    trade_date VARCHAR,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    pre_close DOUBLE,
    change DOUBLE,
    pct_change DOUBLE,
    vol DOUBLE,
    amount DOUBLE,
    asi_bfq DOUBLE,
    asit_bfq DOUBLE,
    atr_bfq DOUBLE,
    bbi_bfq DOUBLE,
    bias1_bfq DOUBLE,
    bias2_bfq DOUBLE,
    bias3_bfq DOUBLE,
    boll_lower_bfq DOUBLE,
    boll_mid_bfq DOUBLE,
    boll_upper_bfq DOUBLE,
    brar_ar_bfq DOUBLE,
    brar_br_bfq DOUBLE,
    cci_bfq DOUBLE,
    cr_bfq DOUBLE,
    dfma_dif_bfq DOUBLE,
    dfma_difma_bfq DOUBLE,
    dmi_adx_bfq DOUBLE,
    dmi_adxr_bfq DOUBLE,
    dmi_mdi_bfq DOUBLE,
    dmi_pdi_bfq DOUBLE,
    downdays DOUBLE,
    updays DOUBLE,
    dpo_bfq DOUBLE,
    madpo_bfq DOUBLE,
    ema_bfq_10 DOUBLE,
    ema_bfq_20 DOUBLE,
    ema_bfq_250 DOUBLE,
    ema_bfq_30 DOUBLE,
    ema_bfq_5 DOUBLE,
    ema_bfq_60 DOUBLE,
    ema_bfq_90 DOUBLE,
    emv_bfq DOUBLE,
    maemv_bfq DOUBLE,
    expma_12_bfq DOUBLE,
    expma_50_bfq DOUBLE,
    kdj_bfq DOUBLE,
    kdj_d_bfq DOUBLE,
    kdj_k_bfq DOUBLE,
    ktn_down_bfq DOUBLE,
    ktn_mid_bfq DOUBLE,
    ktn_upper_bfq DOUBLE,
    lowdays DOUBLE,
    topdays DOUBLE,
    ma_bfq_10 DOUBLE,
    ma_bfq_20 DOUBLE,
    ma_bfq_250 DOUBLE,
    ma_bfq_30 DOUBLE,
    ma_bfq_5 DOUBLE,
    ma_bfq_60 DOUBLE,
    ma_bfq_90 DOUBLE,
    macd_bfq DOUBLE,
    macd_dea_bfq DOUBLE,
    macd_dif_bfq DOUBLE,
    mass_bfq DOUBLE,
    ma_mass_bfq DOUBLE,
    mfi_bfq DOUBLE,
    mtm_bfq DOUBLE,
    mtmma_bfq DOUBLE,
    obv_bfq DOUBLE,
    psy_bfq DOUBLE,
    psyma_bfq DOUBLE,
    roc_bfq DOUBLE,
    maroc_bfq DOUBLE,
    rsi_bfq_12 DOUBLE,
    rsi_bfq_24 DOUBLE,
    rsi_bfq_6 DOUBLE,
    taq_down_bfq DOUBLE,
    taq_mid_bfq DOUBLE,
    taq_up_bfq DOUBLE,
    trix_bfq DOUBLE,
    trma_bfq DOUBLE,
    vr_bfq DOUBLE,
    wr_bfq DOUBLE,
    wr1_bfq DOUBLE,
    xsii_td1_bfq DOUBLE,
    xsii_td2_bfq DOUBLE,
    xsii_td3_bfq DOUBLE,
    xsii_td4_bfq DOUBLE,
    PRIMARY KEY (ts_code, trade_date)
);

-- 可转债基础信息
CREATE TABLE IF NOT EXISTS "cb_basic" (
    ts_code VARCHAR,
    bond_full_name VARCHAR,
    bond_short_name VARCHAR,
    cb_code VARCHAR,
    cb_type VARCHAR,
    stk_code VARCHAR,
    stk_short_name VARCHAR,
    maturity DOUBLE,
    par DOUBLE,
    issue_price DOUBLE,
    issue_size DOUBLE,
    remain_size DOUBLE,
    value_date VARCHAR,
    maturity_date VARCHAR,
    rate_type VARCHAR,
    coupon_rate DOUBLE,
    add_rate DOUBLE,
    pay_per_year BIGINT,
    list_date VARCHAR,
    delist_date VARCHAR,
    exchange VARCHAR,
    conv_start_date VARCHAR,
    conv_end_date VARCHAR,
    conv_stop_date VARCHAR,
    first_conv_price DOUBLE,
    conv_price DOUBLE,
    rate_clause VARCHAR,
    put_clause VARCHAR,
    maturity_call_price VARCHAR,
    maturity_put_price VARCHAR,
    call_clause VARCHAR,
    reset_clause VARCHAR,
    conv_clause VARCHAR,
    guarantor VARCHAR,
    guarantee_type VARCHAR,
    issue_rating VARCHAR,
    newest_rating VARCHAR,
    rating_comp VARCHAR,
    PRIMARY KEY (ts_code)
);

-- 基金规模
CREATE TABLE IF NOT EXISTS "fund_share" (
    ts_code VARCHAR,
    trade_date VARCHAR,
    fd_share DOUBLE,
    PRIMARY KEY (ts_code, trade_date)
);

-- 基金技术面因子(专业版)
CREATE TABLE IF NOT EXISTS "fund_factor_pro" (
    ts_code VARCHAR,
    trade_date VARCHAR,
    trade_date_doris VARCHAR,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    pre_close DOUBLE,
    change DOUBLE,
    pct_change DOUBLE,
    vol DOUBLE,
    amount DOUBLE,
    asi_bfq DOUBLE,
    asit_bfq DOUBLE,
    atr_bfq DOUBLE,
    bbi_bfq DOUBLE,
    bias1_bfq DOUBLE,
    bias2_bfq DOUBLE,
    bias3_bfq DOUBLE,
    boll_lower_bfq DOUBLE,
    boll_mid_bfq DOUBLE,
    boll_upper_bfq DOUBLE,
    brar_ar_bfq DOUBLE,
    brar_br_bfq DOUBLE,
    cci_bfq DOUBLE,
    cr_bfq DOUBLE,
    dfma_dif_bfq DOUBLE,
    dfma_difma_bfq DOUBLE,
    dmi_adx_bfq DOUBLE,
    dmi_adxr_bfq DOUBLE,
    dmi_mdi_bfq DOUBLE,
    dmi_pdi_bfq DOUBLE,
    downdays DOUBLE,
    updays DOUBLE,
    dpo_bfq DOUBLE,
    madpo_bfq DOUBLE,
    ema_bfq_10 DOUBLE,
    ema_bfq_20 DOUBLE,
    ema_bfq_250 DOUBLE,
    ema_bfq_30 DOUBLE,
    ema_bfq_5 DOUBLE,
    ema_bfq_60 DOUBLE,
    ema_bfq_90 DOUBLE,
    emv_bfq DOUBLE,
    maemv_bfq DOUBLE,
    expma_12_bfq DOUBLE,
    expma_50_bfq DOUBLE,
    kdj_bfq DOUBLE,
    kdj_d_bfq DOUBLE,
    kdj_k_bfq DOUBLE,
    ktn_down_bfq DOUBLE,
    ktn_mid_bfq DOUBLE,
    ktn_upper_bfq DOUBLE,
    lowdays DOUBLE,
    topdays DOUBLE,
    ma_bfq_10 DOUBLE,
    ma_bfq_20 DOUBLE,
    ma_bfq_250 DOUBLE,
    ma_bfq_30 DOUBLE,
    ma_bfq_5 DOUBLE,
    ma_bfq_60 DOUBLE,
    ma_bfq_90 DOUBLE,
    macd_bfq DOUBLE,
    macd_dea_bfq DOUBLE,
    macd_dif_bfq DOUBLE,
    mass_bfq DOUBLE,
    ma_mass_bfq DOUBLE,
    mfi_bfq DOUBLE,
    mtm_bfq DOUBLE,
    mtmma_bfq DOUBLE,
    obv_bfq DOUBLE,
    psy_bfq DOUBLE,
    psyma_bfq DOUBLE,
    roc_bfq DOUBLE,
    maroc_bfq DOUBLE,
    rsi_bfq_12 DOUBLE,
    rsi_bfq_24 DOUBLE,
    rsi_bfq_6 DOUBLE,
    taq_down_bfq DOUBLE,
    taq_mid_bfq DOUBLE,
    taq_up_bfq DOUBLE,
    trix_bfq DOUBLE,
    trma_bfq DOUBLE,
    vr_bfq DOUBLE,
    wr_bfq DOUBLE,
    wr1_bfq DOUBLE,
    xsii_td1_bfq DOUBLE,
    xsii_td2_bfq DOUBLE,
    xsii_td3_bfq DOUBLE,
    xsii_td4_bfq DOUBLE,
    PRIMARY KEY (ts_code, trade_date)
);

-- 广州民间借贷利率
CREATE TABLE IF NOT EXISTS "gz_index" (
    date VARCHAR,
    d10_rate DOUBLE,
    m1_rate DOUBLE,
    m3_rate DOUBLE,
    m6_rate DOUBLE,
    m12_rate DOUBLE,
    long_rate DOUBLE,
    PRIMARY KEY (date)
);

-- 沪深市场每日交易统计
CREATE TABLE IF NOT EXISTS "daily_info" (
    trade_date VARCHAR,
    ts_code VARCHAR,
    ts_name VARCHAR,
    com_count BIGINT,
    total_share DOUBLE,
    float_share DOUBLE,
    total_mv DOUBLE,
    float_mv DOUBLE,
    amount DOUBLE,
    vol DOUBLE,
    trans_count BIGINT,
    pe DOUBLE,
    tr DOUBLE,
    exchange VARCHAR,
    PRIMARY KEY (ts_code, trade_date)
);

-- 申万行业分类
CREATE TABLE IF NOT EXISTS "index_classify" (
    index_code VARCHAR,
    industry_name VARCHAR,
    parent_code VARCHAR,
    level VARCHAR,
    industry_code VARCHAR,
    is_pub VARCHAR,
    src VARCHAR,
    PRIMARY KEY (index_code)
);

-- 深圳市场每日交易情况
CREATE TABLE IF NOT EXISTS "sz_daily_info" (
    trade_date VARCHAR,
    ts_code VARCHAR,
    count BIGINT,
    amount DOUBLE,
    vol VARCHAR,
    total_share DOUBLE,
    total_mv DOUBLE,
    float_share DOUBLE,
    float_mv DOUBLE,
    PRIMARY KEY (ts_code, trade_date)
);

-- 大盘指数每日指标
CREATE TABLE IF NOT EXISTS "index_dailybasic" (
    ts_code VARCHAR,
    trade_date VARCHAR,
    total_mv DOUBLE,
    float_mv DOUBLE,
    total_share DOUBLE,
    float_share DOUBLE,
    free_share DOUBLE,
    turnover_rate DOUBLE,
    turnover_rate_f DOUBLE,
    pe DOUBLE,
    pe_ttm DOUBLE,
    pb DOUBLE,
    PRIMARY KEY (ts_code, trade_date)
);

-- 指数成分和权重
CREATE TABLE IF NOT EXISTS "index_weight" (
    index_code VARCHAR,
    con_code VARCHAR,
    trade_date VARCHAR,
    weight DOUBLE,
    PRIMARY KEY (index_code, con_code, trade_date)
);

-- 中信行业成分
CREATE TABLE IF NOT EXISTS "ci_index_member" (
    l1_code VARCHAR,
    l1_name VARCHAR,
    l2_code VARCHAR,
    l2_name VARCHAR,
    l3_code VARCHAR,
    l3_name VARCHAR,
    ts_code VARCHAR,
    name VARCHAR,
    in_date VARCHAR,
    out_date VARCHAR,
    is_new VARCHAR,
    PRIMARY KEY (ts_code)
);

-- 指数技术面因子(专业版)
CREATE TABLE IF NOT EXISTS "idx_factor_pro" (
    ts_code VARCHAR,
    trade_date VARCHAR,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    pre_close DOUBLE,
    change DOUBLE,
    pct_change DOUBLE,
    vol DOUBLE,
    amount DOUBLE,
    asi_bfq DOUBLE,
    asit_bfq DOUBLE,
    atr_bfq DOUBLE,
    bbi_bfq DOUBLE,
    bias1_bfq DOUBLE,
    bias2_bfq DOUBLE,
    bias3_bfq DOUBLE,
    boll_lower_bfq DOUBLE,
    boll_mid_bfq DOUBLE,
    boll_upper_bfq DOUBLE,
    brar_ar_bfq DOUBLE,
    brar_br_bfq DOUBLE,
    cci_bfq DOUBLE,
    cr_bfq DOUBLE,
    dfma_dif_bfq DOUBLE,
    dfma_difma_bfq DOUBLE,
    dmi_adx_bfq DOUBLE,
    dmi_adxr_bfq DOUBLE,
    dmi_mdi_bfq DOUBLE,
    dmi_pdi_bfq DOUBLE,
    downdays DOUBLE,
    updays DOUBLE,
    dpo_bfq DOUBLE,
    madpo_bfq DOUBLE,
    ema_bfq_10 DOUBLE,
    ema_bfq_20 DOUBLE,
    ema_bfq_250 DOUBLE,
    ema_bfq_30 DOUBLE,
    ema_bfq_5 DOUBLE,
    ema_bfq_60 DOUBLE,
    ema_bfq_90 DOUBLE,
    emv_bfq DOUBLE,
    maemv_bfq DOUBLE,
    expma_12_bfq DOUBLE,
    expma_50_bfq DOUBLE,
    kdj_bfq DOUBLE,
    kdj_d_bfq DOUBLE,
    kdj_k_bfq DOUBLE,
    ktn_down_bfq DOUBLE,
    ktn_mid_bfq DOUBLE,
    ktn_upper_bfq DOUBLE,
    lowdays DOUBLE,
    topdays DOUBLE,
    ma_bfq_10 DOUBLE,
    ma_bfq_20 DOUBLE,
    ma_bfq_250 DOUBLE,
    ma_bfq_30 DOUBLE,
    ma_bfq_5 DOUBLE,
    ma_bfq_60 DOUBLE,
    ma_bfq_90 DOUBLE,
    macd_bfq DOUBLE,
    macd_dea_bfq DOUBLE,
    macd_dif_bfq DOUBLE,
    mass_bfq DOUBLE,
    ma_mass_bfq DOUBLE,
    mfi_bfq DOUBLE,
    mtm_bfq DOUBLE,
    mtmma_bfq DOUBLE,
    obv_bfq DOUBLE,
    psy_bfq DOUBLE,
    psyma_bfq DOUBLE,
    roc_bfq DOUBLE,
    maroc_bfq DOUBLE,
    rsi_bfq_12 DOUBLE,
    rsi_bfq_24 DOUBLE,
    rsi_bfq_6 DOUBLE,
    taq_down_bfq DOUBLE,
    taq_mid_bfq DOUBLE,
    taq_up_bfq DOUBLE,
    trix_bfq DOUBLE,
    trma_bfq DOUBLE,
    vr_bfq DOUBLE,
    wr_bfq DOUBLE,
    wr1_bfq DOUBLE,
    xsii_td1_bfq DOUBLE,
    xsii_td2_bfq DOUBLE,
    xsii_td3_bfq DOUBLE,
    xsii_td4_bfq DOUBLE,
    PRIMARY KEY (ts_code, trade_date)
);

-- 申万行业成分(分级)
CREATE TABLE IF NOT EXISTS "index_member_all" (
    l1_code VARCHAR,
    l1_name VARCHAR,
    l2_code VARCHAR,
    l2_name VARCHAR,
    l3_code VARCHAR,
    l3_name VARCHAR,
    ts_code VARCHAR,
    name VARCHAR,
    in_date VARCHAR,
    out_date VARCHAR,
    is_new VARCHAR,
    PRIMARY KEY (ts_code)
);

-- 申万行业指数日行情
CREATE TABLE IF NOT EXISTS "sw_daily" (
    ts_code VARCHAR,
    trade_date VARCHAR,
    name VARCHAR,
    open DOUBLE,
    low DOUBLE,
    high DOUBLE,
    close DOUBLE,
    change DOUBLE,
    pct_change DOUBLE,
    vol DOUBLE,
    amount DOUBLE,
    pe DOUBLE,
    pb DOUBLE,
    float_mv DOUBLE,
    total_mv DOUBLE,
    PRIMARY KEY (ts_code, trade_date)
);

-- 指数基本信息
CREATE TABLE IF NOT EXISTS "index_basic" (
    ts_code VARCHAR,
    name VARCHAR,
    fullname VARCHAR,
    market VARCHAR,
    publisher VARCHAR,
    index_type VARCHAR,
    category VARCHAR,
    base_date VARCHAR,
    base_point DOUBLE,
    list_date VARCHAR,
    weight_rule VARCHAR,
    "desc" VARCHAR,
    exp_date VARCHAR,
    PRIMARY KEY (ts_code)
);

-- 融资融券交易明细
CREATE TABLE IF NOT EXISTS "margin_detail" (
    trade_date VARCHAR,
    ts_code VARCHAR,
    name VARCHAR,
    rzye DOUBLE,
    rqye DOUBLE,
    rzmre DOUBLE,
    rqyl DOUBLE,
    rzche DOUBLE,
    rqchl DOUBLE,
    rqmcl DOUBLE,
    rzrqye DOUBLE,
    PRIMARY KEY (ts_code, trade_date)
);

-- 融资融券交易汇总
CREATE TABLE IF NOT EXISTS "margin" (
    trade_date VARCHAR,
    exchange_id VARCHAR,
    rzye DOUBLE,
    rzmre DOUBLE,
    rzche DOUBLE,
    rqye DOUBLE,
    rqmcl DOUBLE,
    rzrqye DOUBLE,
    rqyl DOUBLE,
    PRIMARY KEY (exchange_id, trade_date)
);

-- 股票回购
CREATE TABLE IF NOT EXISTS "repurchase" (
    ts_code VARCHAR,
    ann_date VARCHAR,
    end_date VARCHAR,
    proc VARCHAR,
    exp_date VARCHAR,
    vol DOUBLE,
    amount DOUBLE,
    high_limit DOUBLE,
    low_limit DOUBLE,
    PRIMARY KEY (ts_code, ann_date)
);

-- 大宗交易
CREATE TABLE IF NOT EXISTS "block_trade" (
    ts_code VARCHAR,
    trade_date VARCHAR,
    price DOUBLE,
    vol DOUBLE,
    amount DOUBLE,
    buyer VARCHAR,
    seller VARCHAR,
    PRIMARY KEY (ts_code, trade_date)
);

-- 股东人数
CREATE TABLE IF NOT EXISTS "stk_holdernumber" (
    ts_code VARCHAR,
    ann_date VARCHAR,
    end_date VARCHAR,
    holder_num BIGINT
);

-- 股东增减持
CREATE TABLE IF NOT EXISTS "stk_holdertrade" (
    ts_code VARCHAR,
    ann_date VARCHAR,
    holder_name VARCHAR,
    holder_type VARCHAR,
    in_de VARCHAR,
    change_vol DOUBLE,
    change_ratio DOUBLE,
    after_share DOUBLE,
    after_ratio DOUBLE,
    avg_price DOUBLE,
    total_share DOUBLE,
    begin_date VARCHAR,
    close_date VARCHAR
);

-- 股权质押明细数据
CREATE TABLE IF NOT EXISTS "pledge_detail" (
    ts_code VARCHAR,
    ann_date VARCHAR,
    holder_name VARCHAR,
    pledge_amount DOUBLE,
    start_date VARCHAR,
    end_date VARCHAR,
    is_release VARCHAR,
    release_date VARCHAR,
    pledgor VARCHAR,
    holding_amount DOUBLE,
    pledged_amount DOUBLE,
    p_total_ratio DOUBLE,
    h_total_ratio DOUBLE,
    is_buyback VARCHAR
);

-- 前十大流通股东
CREATE TABLE IF NOT EXISTS "top10_floatholders" (
    ts_code VARCHAR,
    ann_date VARCHAR,
    end_date VARCHAR,
    holder_name VARCHAR,
    hold_amount DOUBLE,
    hold_ratio DOUBLE,
    hold_float_ratio DOUBLE,
    hold_change DOUBLE,
    holder_type VARCHAR
);

-- 沪深港通股票列表
CREATE TABLE IF NOT EXISTS "stock_hsgt" (
    ts_code VARCHAR,
    trade_date VARCHAR,
    type VARCHAR,
    name VARCHAR,
    type_name VARCHAR,
    PRIMARY KEY (ts_code, trade_date)
);

-- ST股票列表
CREATE TABLE IF NOT EXISTS "stock_st" (
    ts_code VARCHAR,
    name VARCHAR,
    trade_date VARCHAR,
    type VARCHAR,
    type_name VARCHAR,
    PRIMARY KEY (ts_code, trade_date)
);

-- 交易日历
CREATE TABLE IF NOT EXISTS "trade_cal" (
    exchange VARCHAR,
    cal_date VARCHAR,
    is_open VARCHAR,
    pretrade_date VARCHAR
);

-- 股票列表
CREATE TABLE IF NOT EXISTS "stock_basic" (
    ts_code VARCHAR,
    symbol VARCHAR,
    name VARCHAR,
    area VARCHAR,
    industry VARCHAR,
    fullname VARCHAR,
    enname VARCHAR,
    cnspell VARCHAR,
    market VARCHAR,
    exchange VARCHAR,
    curr_type VARCHAR,
    list_status VARCHAR,
    list_date VARCHAR,
    delist_date VARCHAR,
    is_hs VARCHAR,
    act_name VARCHAR,
    act_ent_type VARCHAR,
    PRIMARY KEY (ts_code)
);

-- 股票历史列表
CREATE TABLE IF NOT EXISTS "bak_basic" (
    trade_date VARCHAR,
    ts_code VARCHAR,
    name VARCHAR,
    industry VARCHAR,
    area VARCHAR,
    pe DOUBLE,
    float_share DOUBLE,
    total_share DOUBLE,
    total_assets DOUBLE,
    liquid_assets DOUBLE,
    fixed_assets DOUBLE,
    reserved DOUBLE,
    reserved_pershare DOUBLE,
    eps DOUBLE,
    bvps DOUBLE,
    pb DOUBLE,
    list_date VARCHAR,
    undp DOUBLE,
    per_undp DOUBLE,
    rev_yoy DOUBLE,
    profit_yoy DOUBLE,
    gpr DOUBLE,
    npr DOUBLE,
    holder_num BIGINT,
    PRIMARY KEY (ts_code, trade_date)
);

-- 涨跌停和炸板数据
CREATE TABLE IF NOT EXISTS "limit_list_d" (
    trade_date VARCHAR,
    ts_code VARCHAR,
    industry VARCHAR,
    name VARCHAR,
    close DOUBLE,
    pct_chg DOUBLE,
    amount DOUBLE,
    limit_amount DOUBLE,
    float_mv DOUBLE,
    total_mv DOUBLE,
    turnover_ratio DOUBLE,
    fd_amount DOUBLE,
    first_time VARCHAR,
    last_time VARCHAR,
    open_times BIGINT,
    up_stat VARCHAR,
    limit_times BIGINT,
    "limit" VARCHAR,
    PRIMARY KEY (ts_code, trade_date)
);

-- 市场游资最全名录
CREATE TABLE IF NOT EXISTS "hm_list" (
    name VARCHAR,
    "desc" VARCHAR,
    orgs VARCHAR
);

-- 榜单数据(开盘啦)
CREATE TABLE IF NOT EXISTS "kpl_list" (
    ts_code VARCHAR,
    name VARCHAR,
    trade_date VARCHAR,
    lu_time VARCHAR,
    ld_time VARCHAR,
    open_time VARCHAR,
    last_time VARCHAR,
    lu_desc VARCHAR,
    tag VARCHAR,
    theme VARCHAR,
    net_change DOUBLE,
    bid_amount DOUBLE,
    status VARCHAR,
    bid_change DOUBLE,
    bid_turnover DOUBLE,
    lu_bid_vol DOUBLE,
    pct_chg DOUBLE,
    bid_pct_chg DOUBLE,
    rt_pct_chg DOUBLE,
    limit_order DOUBLE,
    amount DOUBLE,
    turnover_rate DOUBLE,
    free_float DOUBLE,
    lu_limit_order DOUBLE,
    PRIMARY KEY (ts_code, trade_date)
);

-- 龙虎榜机构交易单
CREATE TABLE IF NOT EXISTS "top_inst" (
    trade_date VARCHAR,
    ts_code VARCHAR,
    exalter VARCHAR,
    side VARCHAR,
    buy DOUBLE,
    buy_rate DOUBLE,
    sell DOUBLE,
    sell_rate DOUBLE,
    net_buy DOUBLE,
    reason VARCHAR
);

-- 题材成分(开盘啦)
CREATE TABLE IF NOT EXISTS "kpl_concept_cons" (
    ts_code VARCHAR,
    name VARCHAR,
    con_name VARCHAR,
    con_code VARCHAR,
    trade_date VARCHAR,
    "desc" VARCHAR,
    hot_num BIGINT,
    PRIMARY KEY (ts_code, trade_date)
);

-- 龙虎榜每日统计单
CREATE TABLE IF NOT EXISTS "top_list" (
    trade_date VARCHAR,
    ts_code VARCHAR,
    name VARCHAR,
    close DOUBLE,
    pct_change DOUBLE,
    turnover_rate DOUBLE,
    amount DOUBLE,
    l_sell DOUBLE,
    l_buy DOUBLE,
    l_amount DOUBLE,
    net_amount DOUBLE,
    net_rate DOUBLE,
    amount_rate DOUBLE,
    float_values DOUBLE,
    reason VARCHAR,
    PRIMARY KEY (ts_code, trade_date)
);

-- 每日筹码及胜率
CREATE TABLE IF NOT EXISTS "cyq_perf" (
    ts_code VARCHAR,
    trade_date VARCHAR,
    his_low DOUBLE,
    his_high DOUBLE,
    cost_5pct DOUBLE,
    cost_15pct DOUBLE,
    cost_50pct DOUBLE,
    cost_85pct DOUBLE,
    cost_95pct DOUBLE,
    weight_avg DOUBLE,
    winner_rate DOUBLE,
    PRIMARY KEY (ts_code, trade_date)
);

-- 股票技术面因子(专业版)
CREATE TABLE IF NOT EXISTS "stk_factor_pro" (
    ts_code VARCHAR,
    trade_date VARCHAR,
    open DOUBLE,
    open_hfq DOUBLE,
    open_qfq DOUBLE,
    high DOUBLE,
    high_hfq DOUBLE,
    high_qfq DOUBLE,
    low DOUBLE,
    low_hfq DOUBLE,
    low_qfq DOUBLE,
    close DOUBLE,
    close_hfq DOUBLE,
    close_qfq DOUBLE,
    pre_close DOUBLE,
    change DOUBLE,
    pct_chg DOUBLE,
    vol DOUBLE,
    amount DOUBLE,
    turnover_rate DOUBLE,
    turnover_rate_f DOUBLE,
    volume_ratio DOUBLE,
    pe DOUBLE,
    pe_ttm DOUBLE,
    pb DOUBLE,
    ps DOUBLE,
    ps_ttm DOUBLE,
    dv_ratio DOUBLE,
    dv_ttm DOUBLE,
    total_share DOUBLE,
    float_share DOUBLE,
    free_share DOUBLE,
    total_mv DOUBLE,
    circ_mv DOUBLE,
    adj_factor DOUBLE,
    ema_bfq_5 DOUBLE,
    ema_hfq_5 DOUBLE,
    ema_qfq_5 DOUBLE,
    ema_bfq_10 DOUBLE,
    ema_hfq_10 DOUBLE,
    ema_qfq_10 DOUBLE,
    ema_bfq_20 DOUBLE,
    ema_hfq_20 DOUBLE,
    ema_qfq_20 DOUBLE,
    ema_bfq_30 DOUBLE,
    ema_hfq_30 DOUBLE,
    ema_qfq_30 DOUBLE,
    ema_bfq_60 DOUBLE,
    ema_hfq_60 DOUBLE,
    ema_qfq_60 DOUBLE,
    ema_bfq_90 DOUBLE,
    ema_hfq_90 DOUBLE,
    ema_qfq_90 DOUBLE,
    ema_bfq_250 DOUBLE,
    ema_hfq_250 DOUBLE,
    ema_qfq_250 DOUBLE,
    ma_bfq_5 DOUBLE,
    ma_hfq_5 DOUBLE,
    ma_qfq_5 DOUBLE,
    ma_bfq_10 DOUBLE,
    ma_hfq_10 DOUBLE,
    ma_qfq_10 DOUBLE,
    ma_bfq_20 DOUBLE,
    ma_hfq_20 DOUBLE,
    ma_qfq_20 DOUBLE,
    ma_bfq_30 DOUBLE,
    ma_hfq_30 DOUBLE,
    ma_qfq_30 DOUBLE,
    ma_bfq_60 DOUBLE,
    ma_hfq_60 DOUBLE,
    ma_qfq_60 DOUBLE,
    ma_bfq_90 DOUBLE,
    ma_hfq_90 DOUBLE,
    ma_qfq_90 DOUBLE,
    ma_bfq_250 DOUBLE,
    ma_hfq_250 DOUBLE,
    ma_qfq_250 DOUBLE,
    rsi_bfq_6 DOUBLE,
    rsi_hfq_6 DOUBLE,
    rsi_qfq_6 DOUBLE,
    rsi_bfq_12 DOUBLE,
    rsi_hfq_12 DOUBLE,
    rsi_qfq_12 DOUBLE,
    rsi_bfq_24 DOUBLE,
    rsi_hfq_24 DOUBLE,
    rsi_qfq_24 DOUBLE,
    asi_bfq DOUBLE,
    asi_hfq DOUBLE,
    asi_qfq DOUBLE,
    asit_bfq DOUBLE,
    asit_hfq DOUBLE,
    asit_qfq DOUBLE,
    atr_bfq DOUBLE,
    atr_hfq DOUBLE,
    atr_qfq DOUBLE,
    bbi_bfq DOUBLE,
    bbi_hfq DOUBLE,
    bbi_qfq DOUBLE,
    bias1_bfq DOUBLE,
    bias1_hfq DOUBLE,
    bias1_qfq DOUBLE,
    bias2_bfq DOUBLE,
    bias2_hfq DOUBLE,
    bias2_qfq DOUBLE,
    bias3_bfq DOUBLE,
    bias3_hfq DOUBLE,
    bias3_qfq DOUBLE,
    boll_lower_bfq DOUBLE,
    boll_lower_hfq DOUBLE,
    boll_lower_qfq DOUBLE,
    boll_mid_bfq DOUBLE,
    boll_mid_hfq DOUBLE,
    boll_mid_qfq DOUBLE,
    boll_upper_bfq DOUBLE,
    boll_upper_hfq DOUBLE,
    boll_upper_qfq DOUBLE,
    brar_ar_bfq DOUBLE,
    brar_ar_hfq DOUBLE,
    brar_ar_qfq DOUBLE,
    brar_br_bfq DOUBLE,
    brar_br_hfq DOUBLE,
    brar_br_qfq DOUBLE,
    cci_bfq DOUBLE,
    cci_hfq DOUBLE,
    cci_qfq DOUBLE,
    cr_bfq DOUBLE,
    cr_hfq DOUBLE,
    cr_qfq DOUBLE,
    dfma_dif_bfq DOUBLE,
    dfma_dif_hfq DOUBLE,
    dfma_dif_qfq DOUBLE,
    dfma_difma_bfq DOUBLE,
    dfma_difma_hfq DOUBLE,
    dfma_difma_qfq DOUBLE,
    dmi_adx_bfq DOUBLE,
    dmi_adx_hfq DOUBLE,
    dmi_adx_qfq DOUBLE,
    dmi_adxr_bfq DOUBLE,
    dmi_adxr_hfq DOUBLE,
    dmi_adxr_qfq DOUBLE,
    dmi_mdi_bfq DOUBLE,
    dmi_mdi_hfq DOUBLE,
    dmi_mdi_qfq DOUBLE,
    dmi_pdi_bfq DOUBLE,
    dmi_pdi_hfq DOUBLE,
    dmi_pdi_qfq DOUBLE,
    downdays_bfq DOUBLE,
    downdays_hfq DOUBLE,
    downdays_qfq DOUBLE,
    updays_bfq DOUBLE,
    updays_hfq DOUBLE,
    updays_qfq DOUBLE,
    dpo_bfq DOUBLE,
    dpo_hfq DOUBLE,
    dpo_qfq DOUBLE,
    madpo_bfq DOUBLE,
    madpo_hfq DOUBLE,
    madpo_qfq DOUBLE,
    emv_bfq DOUBLE,
    emv_hfq DOUBLE,
    emv_qfq DOUBLE,
    maemv_bfq DOUBLE,
    maemv_hfq DOUBLE,
    maemv_qfq DOUBLE,
    expma_12_bfq DOUBLE,
    expma_12_hfq DOUBLE,
    expma_12_qfq DOUBLE,
    expma_50_bfq DOUBLE,
    expma_50_hfq DOUBLE,
    expma_50_qfq DOUBLE,
    kdj_bfq DOUBLE,
    kdj_hfq DOUBLE,
    kdj_qfq DOUBLE,
    kdj_d_bfq DOUBLE,
    kdj_d_hfq DOUBLE,
    kdj_d_qfq DOUBLE,
    kdj_k_bfq DOUBLE,
    kdj_k_hfq DOUBLE,
    kdj_k_qfq DOUBLE,
    ktn_down_bfq DOUBLE,
    ktn_down_hfq DOUBLE,
    ktn_down_qfq DOUBLE,
    ktn_mid_bfq DOUBLE,
    ktn_mid_hfq DOUBLE,
    ktn_mid_qfq DOUBLE,
    ktn_upper_bfq DOUBLE,
    ktn_upper_hfq DOUBLE,
    ktn_upper_qfq DOUBLE,
    lowdays_bfq DOUBLE,
    lowdays_hfq DOUBLE,
    lowdays_qfq DOUBLE,
    topdays_bfq DOUBLE,
    topdays_hfq DOUBLE,
    topdays_qfq DOUBLE,
    macd_bfq DOUBLE,
    macd_hfq DOUBLE,
    macd_qfq DOUBLE,
    macd_dea_bfq DOUBLE,
    macd_dea_hfq DOUBLE,
    macd_dea_qfq DOUBLE,
    macd_dif_bfq DOUBLE,
    macd_dif_hfq DOUBLE,
    macd_dif_qfq DOUBLE,
    mass_bfq DOUBLE,
    mass_hfq DOUBLE,
    mass_qfq DOUBLE,
    ma_mass_bfq DOUBLE,
    ma_mass_hfq DOUBLE,
    ma_mass_qfq DOUBLE,
    mfi_bfq DOUBLE,
    mfi_hfq DOUBLE,
    mfi_qfq DOUBLE,
    mtm_bfq DOUBLE,
    mtm_hfq DOUBLE,
    mtm_qfq DOUBLE,
    mtmma_bfq DOUBLE,
    mtmma_hfq DOUBLE,
    mtmma_qfq DOUBLE,
    obv_bfq DOUBLE,
    obv_hfq DOUBLE,
    obv_qfq DOUBLE,
    psy_bfq DOUBLE,
    psy_hfq DOUBLE,
    psy_qfq DOUBLE,
    psyma_bfq DOUBLE,
    psyma_hfq DOUBLE,
    psyma_qfq DOUBLE,
    roc_bfq DOUBLE,
    roc_hfq DOUBLE,
    roc_qfq DOUBLE,
    maroc_bfq DOUBLE,
    maroc_hfq DOUBLE,
    maroc_qfq DOUBLE,
    taq_down_bfq DOUBLE,
    taq_down_hfq DOUBLE,
    taq_down_qfq DOUBLE,
    taq_mid_bfq DOUBLE,
    taq_mid_hfq DOUBLE,
    taq_mid_qfq DOUBLE,
    taq_up_bfq DOUBLE,
    taq_up_hfq DOUBLE,
    taq_up_qfq DOUBLE,
    trix_bfq DOUBLE,
    trix_hfq DOUBLE,
    trix_qfq DOUBLE,
    trma_bfq DOUBLE,
    trma_hfq DOUBLE,
    trma_qfq DOUBLE,
    vr_bfq DOUBLE,
    vr_hfq DOUBLE,
    vr_qfq DOUBLE,
    wr_bfq DOUBLE,
    wr_hfq DOUBLE,
    wr_qfq DOUBLE,
    wr1_bfq DOUBLE,
    wr1_hfq DOUBLE,
    wr1_qfq DOUBLE,
    xsii_td1_bfq DOUBLE,
    xsii_td1_hfq DOUBLE,
    xsii_td1_qfq DOUBLE,
    xsii_td2_bfq DOUBLE,
    xsii_td2_hfq DOUBLE,
    xsii_td2_qfq DOUBLE,
    xsii_td3_bfq DOUBLE,
    xsii_td3_hfq DOUBLE,
    xsii_td3_qfq DOUBLE,
    xsii_td4_bfq DOUBLE,
    xsii_td4_hfq DOUBLE,
    xsii_td4_qfq DOUBLE,
    PRIMARY KEY (ts_code, trade_date)
);

-- 备用行情
CREATE TABLE IF NOT EXISTS "bak_daily" (
    ts_code VARCHAR,
    trade_date VARCHAR,
    name VARCHAR,
    pct_change DOUBLE,
    close DOUBLE,
    change DOUBLE,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    pre_close DOUBLE,
    vol_ratio DOUBLE,
    turn_over DOUBLE,
    swing DOUBLE,
    vol DOUBLE,
    amount DOUBLE,
    selling DOUBLE,
    buying DOUBLE,
    total_share DOUBLE,
    float_share DOUBLE,
    pe DOUBLE,
    industry VARCHAR,
    area VARCHAR,
    float_mv DOUBLE,
    total_mv DOUBLE,
    avg_price DOUBLE,
    strength DOUBLE,
    activity DOUBLE,
    avg_turnover DOUBLE,
    attack DOUBLE,
    interval_3 DOUBLE,
    interval_6 DOUBLE,
    PRIMARY KEY (ts_code, trade_date)
);

-- 每日涨跌停价格
CREATE TABLE IF NOT EXISTS "stk_limit" (
    trade_date VARCHAR,
    ts_code VARCHAR,
    pre_close DOUBLE,
    up_limit DOUBLE,
    down_limit DOUBLE,
    PRIMARY KEY (ts_code, trade_date)
);

-- 分红送股数据
CREATE TABLE IF NOT EXISTS "dividend" (
    ts_code VARCHAR,
    end_date VARCHAR,
    ann_date VARCHAR,
    div_proc VARCHAR,
    stk_div DOUBLE,
    stk_bo_rate DOUBLE,
    stk_co_rate DOUBLE,
    cash_div DOUBLE,
    cash_div_tax DOUBLE,
    record_date VARCHAR,
    ex_date VARCHAR,
    pay_date VARCHAR,
    div_listdate VARCHAR,
    imp_ann_date VARCHAR,
    base_date VARCHAR,
    base_share DOUBLE
);

-- 沪深港通资金流向
CREATE TABLE IF NOT EXISTS "moneyflow_hsgt" (
    trade_date VARCHAR,
    ggt_ss DOUBLE,
    ggt_sz DOUBLE,
    hgt DOUBLE,
    sgt DOUBLE,
    north_money DOUBLE,
    south_money DOUBLE,
    PRIMARY KEY (trade_date)
);

-- 个股资金流向
CREATE TABLE IF NOT EXISTS "moneyflow" (
    ts_code VARCHAR,
    trade_date VARCHAR,
    buy_sm_vol BIGINT,
    buy_sm_amount DOUBLE,
    sell_sm_vol BIGINT,
    sell_sm_amount DOUBLE,
    buy_md_vol BIGINT,
    buy_md_amount DOUBLE,
    sell_md_vol BIGINT,
    sell_md_amount DOUBLE,
    buy_lg_vol BIGINT,
    buy_lg_amount DOUBLE,
    sell_lg_vol BIGINT,
    sell_lg_amount DOUBLE,
    buy_elg_vol BIGINT,
    buy_elg_amount DOUBLE,
    sell_elg_vol BIGINT,
    sell_elg_amount DOUBLE,
    net_mf_vol BIGINT,
    net_mf_amount DOUBLE,
    PRIMARY KEY (ts_code, trade_date)
);

-- 个股资金流向(DC)
CREATE TABLE IF NOT EXISTS "moneyflow_dc" (
    trade_date VARCHAR,
    ts_code VARCHAR,
    name VARCHAR,
    pct_change DOUBLE,
    close DOUBLE,
    net_amount DOUBLE,
    net_amount_rate DOUBLE,
    buy_elg_amount DOUBLE,
    buy_elg_amount_rate DOUBLE,
    buy_lg_amount DOUBLE,
    buy_lg_amount_rate DOUBLE,
    buy_md_amount DOUBLE,
    buy_md_amount_rate DOUBLE,
    buy_sm_amount DOUBLE,
    buy_sm_amount_rate DOUBLE,
    PRIMARY KEY (ts_code, trade_date)
);

-- 现金分红事件（ex_date 作 trade_date，仅除息日有值）
DROP VIEW IF EXISTS dividend_grid;
CREATE VIEW dividend_grid AS
  SELECT ts_code, ex_date AS trade_date, SUM(cash_div) AS cash_div
  FROM dividend
  WHERE div_proc='实施' AND cash_div > 0 AND ex_date IS NOT NULL
  GROUP BY ts_code, ex_date;

-- 股东人数（同公告取 MAX 披露口径）
DROP VIEW IF EXISTS stk_holdernumber_agg;
CREATE VIEW stk_holdernumber_agg AS
  SELECT ts_code, ann_date, MAX(holder_num) AS holder_num
  FROM stk_holdernumber WHERE holder_num IS NOT NULL GROUP BY ts_code, ann_date;

-- 股东增减持（change_vol/change_ratio 按 in_de 带符号，DE=负；
-- change_ratio 为绝对变动股数加权的带符号平均比例，分母仍为绝对股数之和）
DROP VIEW IF EXISTS stk_holdertrade_agg;
CREATE VIEW stk_holdertrade_agg AS
  SELECT ts_code, ann_date,
         SUM(CASE WHEN in_de = 'DE' THEN -change_vol ELSE change_vol END) AS change_vol,
         SUM(CASE WHEN in_de = 'DE' THEN -change_vol * change_ratio
                  ELSE change_vol * change_ratio END)
             / NULLIF(SUM(change_vol), 0) AS change_ratio
  FROM stk_holdertrade
  GROUP BY ts_code, ann_date;

-- 股权质押（同公告取 MAX 披露比例，保守口径）
DROP VIEW IF EXISTS pledge_detail_agg;
CREATE VIEW pledge_detail_agg AS
  SELECT ts_code, ann_date,
         MAX(p_total_ratio) AS p_total_ratio,
         MAX(h_total_ratio) AS h_total_ratio
  FROM pledge_detail
  GROUP BY ts_code, ann_date;

-- 龙虎榜机构席位聚合（top_inst 为逐席位多行，2026-09 起不再折叠：
-- 原 PK(ts_code, trade_date) 会把多席位 INSERT OR REPLACE 成任意单行）。
-- 机构口径 = exalter='机构专用'；inst_buy_rate = 机构买入额 / 全体上榜席位买入额。
DROP VIEW IF EXISTS top_inst_agg;
CREATE VIEW top_inst_agg AS
  SELECT ts_code, trade_date,
         SUM(CASE WHEN exalter = '机构专用' THEN net_buy ELSE 0 END) AS inst_net_buy,
         SUM(CASE WHEN exalter = '机构专用' THEN buy ELSE 0 END) AS inst_buy,
         SUM(buy) AS total_buy,
         SUM(CASE WHEN exalter = '机构专用' THEN buy ELSE 0 END)
             / NULLIF(SUM(buy), 0) AS inst_buy_rate
  FROM top_inst
  GROUP BY ts_code, trade_date;
