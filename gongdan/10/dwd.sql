
-- ==================================1.页面访问明细表 dwd_page_visit_detail=====================================================================



-- 1.页面访问明细表（清洗后，关联页面和时间维度）
CREATE TABLE dwd_page_visit_detail (
    log_id BIGINT COMMENT '日志ID，关联ODS层log_id',
    user_id BIGINT COMMENT '用户ID',
    page_id INT COMMENT '页面ID',
    page_name STRING COMMENT '页面名称（关联dim_page_info）',
    page_type STRING COMMENT '页面类型',
    visit_time DATETIME COMMENT '访问时间',
    stay_duration INT COMMENT '停留时长（秒）',
    stat_date DATE COMMENT '统计日期（关联dim_time_info）'
)
DUPLICATE KEY(log_id)
PARTITION BY RANGE (stat_date) (
    PARTITION p202501 VALUES LESS THAN ('2025-02-01')
)
DISTRIBUTED BY HASH(page_id) BUCKETS 16
PROPERTIES (
    "storage_type" = "COLUMN",
    "replication_num" = "1"
);


-- 插入访问明细数据（过滤无效访问，关联维度）
INSERT INTO dwd_page_visit_detail
SELECT 
    l.log_id,
    l.user_id,
    l.page_id,
    p.page_name,
    p.page_type,
    l.log_time AS visit_time,
    l.stay_duration,
    DATE(l.log_time) AS stat_date
FROM ods_page_log l
JOIN dim_page_info p ON l.page_id = p.page_id
WHERE l.stay_duration >= 10;  -- 过滤无效停留


-- ==================================2.页面点击明细表 dwd_page_click_detail=====================================================================


-- 2.页面点击明细表（提取点击行为，支撑装修诊断分析）
CREATE TABLE dwd_page_click_detail (
    log_id BIGINT COMMENT '日志ID，关联ODS层log_id',
    user_id BIGINT COMMENT '用户ID',
    page_id INT COMMENT '页面ID',
    click_section STRING COMMENT '点击板块（导航栏/活动Banner等）',
    click_count INT COMMENT '板块点击次数',
    guide_pay_amount DECIMAL(10,2) COMMENT '引导支付金额',
    stat_date DATE COMMENT '统计日期（关联dim_time_info）'
)
DUPLICATE KEY(log_id)
PARTITION BY RANGE (stat_date) (
    PARTITION p202501 VALUES LESS THAN ('2025-02-01')
)
DISTRIBUTED BY HASH(page_id) BUCKETS 16
PROPERTIES (
    "storage_type" = "COLUMN",
    "replication_num" = "1"
);


-- 插入点击明细数据（提取有效点击行为）
INSERT INTO dwd_page_click_detail
SELECT 
    log_id,
    user_id,
    page_id,
    click_section,
    click_count,
    guide_pay_amount,
    DATE(log_time) AS stat_date
FROM ods_page_log
WHERE click_count > 0;  -- 仅保留有点击的记录