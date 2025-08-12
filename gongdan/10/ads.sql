


-- ==================================1.页面分析看板指标表 ads_page_analysis_board=====================================================================

-- 1. 页面分析看板指标表
CREATE TABLE ads_page_analysis_board (
    page_id INT COMMENT '页面ID',
    stat_date DATE COMMENT '统计日期',
    page_name STRING COMMENT '页面名称',
    page_type STRING COMMENT '页面类型',
    pv BIGINT COMMENT '访问量',
    uv BIGINT COMMENT '访客数',
    total_click INT COMMENT '总点击量',
    click_rate DECIMAL(8,2) COMMENT '点击率（总点击量/访问量）',  -- 扩大精度至8位总长度，避免1900+数值溢出
    total_guide_pay DECIMAL(12,2) COMMENT '引导支付总额',
    top_click_section STRING COMMENT '点击量最高的板块',
    last_30d_uv_trend STRING COMMENT '近30天UV趋势（日期:UV,日期:UV...）'
)
DUPLICATE KEY(page_id, stat_date)
PARTITION BY RANGE (stat_date) (
    PARTITION p202501 VALUES LESS THAN ('2025-02-01')
)
DISTRIBUTED BY HASH(page_id) BUCKETS 4
PROPERTIES (
    "storage_type" = "COLUMN",
    "replication_num" = "1"
);

-- 2. 调整INSERT语句的字段顺序，与表结构列顺序严格一致
INSERT INTO ads_page_analysis_board (
    page_id, stat_date, page_name, page_type, pv, uv, 
    total_click, click_rate, total_guide_pay, top_click_section, last_30d_uv_trend
)
SELECT 
    v.page_id,
    v.stat_date,  -- 对应表结构的第2列stat_date
    p.page_name,
    p.page_type,
    v.pv,
    v.uv,
    c.total_click,
    ROUND(c.total_click / v.pv * 100, 2) AS click_rate,  -- 计算结果适配DECIMAL(8,2)
    c.total_guide_pay,
    c.top_click_section,
    GROUP_CONCAT(DISTINCT CONCAT(t.stat_date, ':', t.visitor_count) ORDER BY t.stat_date) AS last_30d_uv_trend
FROM 
    dws_page_visit_summary v
JOIN dim_page_info p ON v.page_id = p.page_id
JOIN dws_page_click_summary c ON v.page_id = c.page_id AND v.stat_date = c.stat_date
LEFT JOIN dws_page_trend_summary t 
    ON v.page_id = t.page_id 
    AND t.stat_date BETWEEN DATE_SUB(v.stat_date, INTERVAL 29 DAY) AND v.stat_date
GROUP BY 
    v.page_id, v.stat_date, p.page_name, p.page_type, v.pv, v.uv, c.total_click, c.total_guide_pay, c.top_click_section;