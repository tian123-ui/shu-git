
-- ==================================1.页面访问汇总 dws_page_visit_summary=====================================================================


-- 1.页面访问汇总（计算PV、UV等核心指标）
CREATE TABLE dws_page_visit_summary (
    page_id INT COMMENT '页面ID',
    stat_date DATE COMMENT '统计日期',
    pv BIGINT COMMENT '访问量',
    uv BIGINT COMMENT '访客数',
    avg_stay_duration DECIMAL(5,2) COMMENT '平均停留时长（秒）'
)
DUPLICATE KEY(page_id, stat_date)
PARTITION BY RANGE (stat_date) (
    PARTITION p202501 VALUES LESS THAN ('2025-02-01')
)
DISTRIBUTED BY HASH(page_id) BUCKETS 8
PROPERTIES (
    "storage_type" = "COLUMN",
    "replication_num" = "1"
);

-- 插入访问汇总数据
INSERT INTO dws_page_visit_summary
SELECT 
    page_id,
    stat_date,
    COUNT(log_id) AS pv,
    COUNT(DISTINCT user_id) AS uv,
    AVG(stay_duration) AS avg_stay_duration
FROM dwd_page_visit_detail
GROUP BY page_id, stat_date;



-- ==================================2.页面点击汇总 dws_page_click_summary=====================================================================

-- 2.页面点击汇总（支撑点击分布与引导效果分析）
CREATE TABLE dws_page_click_summary (
    page_id INT COMMENT '页面ID',
    stat_date DATE COMMENT '统计日期',
    total_click INT COMMENT '总点击量',
    click_users BIGINT COMMENT '点击人数',
    total_guide_pay DECIMAL(12,2) COMMENT '引导支付总额',
    top_click_section STRING COMMENT '点击量最高的板块'
)
DUPLICATE KEY(page_id, stat_date)
PARTITION BY RANGE (stat_date) (
    PARTITION p202501 VALUES LESS THAN ('2025-02-01')
)
DISTRIBUTED BY HASH(page_id) BUCKETS 8
PROPERTIES (
    "storage_type" = "COLUMN",
    "replication_num" = "1"
);

-- 插入点击汇总数据
INSERT INTO dws_page_click_summary
SELECT 
    page_id,
    stat_date,
    SUM(click_count) AS total_click,
    COUNT(DISTINCT user_id) AS click_users,
    SUM(guide_pay_amount) AS total_guide_pay,
    MAX_BY(click_section, click_count) AS top_click_section
FROM dwd_page_click_detail
GROUP BY page_id, stat_date;



-- ==================================3.页面趋势汇总 dws_page_trend_summary=====================================================================

-- 3.页面趋势汇总（支撑近30天数据趋势分析）
CREATE TABLE dws_page_trend_summary (
    page_id INT COMMENT '页面ID',
    stat_date DATE COMMENT '统计日期',
    visitor_count BIGINT COMMENT '访客数',
    click_count BIGINT COMMENT '点击人数'
)
DUPLICATE KEY(page_id, stat_date)
PARTITION BY RANGE (stat_date) (
    PARTITION p202501 VALUES LESS THAN ('2025-02-01')
)
DISTRIBUTED BY HASH(page_id) BUCKETS 8
PROPERTIES (
    "storage_type" = "COLUMN",
    "replication_num" = "1"
);

-- 插入趋势汇总数据
INSERT INTO dws_page_trend_summary
SELECT 
    v.page_id,
    v.stat_date,
    v.uv AS visitor_count,
    c.click_users AS click_count
FROM dws_page_visit_summary v
JOIN dws_page_click_summary c 
    ON v.page_id = c.page_id AND v.stat_date = c.stat_date;
