
-- ==================================1.存储页面访问原始日志 ods_page_log================================================================================



-- ODS层：存储页面访问原始日志
CREATE TABLE ods_page_log (
    log_time DATETIME COMMENT '访问时间',
    page_id INT COMMENT '页面ID',
    log_id BIGINT COMMENT '日志唯一ID',
    user_id BIGINT COMMENT '用户ID',
    page_type STRING COMMENT '页面类型（首页/自定义承接页/商品详情页）',
    click_section STRING COMMENT '点击板块（导航栏/活动Banner等）',
    click_count INT COMMENT '板块点击次数',
    guide_pay_amount DECIMAL(10,2) COMMENT '引导支付金额',
    stay_duration INT COMMENT '停留时长（秒）'
)
DUPLICATE KEY(log_time, page_id)
PARTITION BY RANGE (log_time) (
    PARTITION p202501 VALUES LESS THAN ('2025-02-01')
)
DISTRIBUTED BY HASH(page_id) BUCKETS 32
PROPERTIES (
    "storage_type" = "COLUMN",
    "replication_num" = "1"
);


-- 方案：通过多张小表交叉连接生成序列，分批次插入数据

-- 第一次插入50万条（利用3张表交叉连接生成50万+数据）
INSERT INTO ods_page_log
SELECT 
    -- 生成随机访问时间
    DATE_ADD('2025-01-01', INTERVAL FLOOR(RAND()*31) DAY) + 
    INTERVAL FLOOR(RAND()*24) HOUR +
    INTERVAL FLOOR(RAND()*60) MINUTE AS log_time,
    -- 生成页面ID（1-50）
    FLOOR(RAND()*50)+1 AS page_id,
    -- 生成唯一日志ID（1-500000）
    t1.id + t2.id * 1000 + t3.id * 1000000 AS log_id,
    -- 生成用户ID（1-50000）
    FLOOR(RAND()*50000)+1 AS user_id,
    -- 生成页面类型
    CASE FLOOR(RAND()*3) 
        WHEN 0 THEN '首页' 
        WHEN 1 THEN '自定义承接页' 
        ELSE '商品详情页' 
    END AS page_type,
    -- 生成点击板块
    CASE FLOOR(RAND()*4) 
        WHEN 0 THEN '导航栏' 
        WHEN 1 THEN '活动Banner' 
        WHEN 2 THEN '商品推荐' 
        ELSE '评价区' 
    END AS click_section,
    -- 生成点击次数（1-20）
    FLOOR(RAND()*20)+1 AS click_count,
    -- 生成引导支付金额（0-500，保留两位小数）
    ROUND(RAND()*500, 2) AS guide_pay_amount,
    -- 生成停留时长（30-330秒）
    FLOOR(RAND()*300)+30 AS stay_duration
FROM 
    -- 生成基础序列表t1（1-1000）
    (SELECT 1 AS id UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 
     UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10
     -- 省略中间值，实际使用时补充到1000）
     UNION SELECT 999 UNION SELECT 1000) t1,
    -- 生成序列表t2（1-500），与t1交叉连接生成50万数据（1000*500）
    (SELECT 1 AS id UNION SELECT 2 UNION SELECT 3 
     -- 省略中间值，实际使用时补充到500）
     UNION SELECT 499 UNION SELECT 500) t2,
    -- 限制总条数为50万
    (SELECT 1 AS id LIMIT 1) t3
LIMIT 500000;


-- 第二次插入50万条（日志ID从500001开始）
INSERT INTO ods_page_log
SELECT 
    DATE_ADD('2025-01-01', INTERVAL FLOOR(RAND()*31) DAY) + 
    INTERVAL FLOOR(RAND()*24) HOUR +
    INTERVAL FLOOR(RAND()*60) MINUTE AS log_time,
    FLOOR(RAND()*50)+1 AS page_id,
    -- 日志ID偏移50万，避免重复
    t1.id + t2.id * 1000 + t3.id * 1000000 + 500000 AS log_id,
    FLOOR(RAND()*50000)+1 AS user_id,
    CASE FLOOR(RAND()*3) 
        WHEN 0 THEN '首页' 
        WHEN 1 THEN '自定义承接页' 
        ELSE '商品详情页' 
    END AS page_type,
    CASE FLOOR(RAND()*4) 
        WHEN 0 THEN '导航栏' 
        WHEN 1 THEN '活动Banner' 
        WHEN 2 THEN '商品推荐' 
        ELSE '评价区' 
    END AS click_section,
    FLOOR(RAND()*20)+1 AS click_count,
    ROUND(RAND()*500, 2) AS guide_pay_amount,
    FLOOR(RAND()*300)+30 AS stay_duration
FROM 
    (SELECT 1 AS id UNION SELECT 2 UNION SELECT 3 
     -- 省略中间值，补充到1000）
     UNION SELECT 999 UNION SELECT 1000) t1,
    (SELECT 1 AS id UNION SELECT 2 UNION SELECT 3 
     -- 省略中间值，补充到500）
     UNION SELECT 499 UNION SELECT 500) t2,
    (SELECT 1 AS id LIMIT 1) t3
LIMIT 500000;


select * from ods_page_log;


