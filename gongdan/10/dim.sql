
-- =====================================1.页面维度信息  dim_page_info=====================================================================================


-- 1.页面维度表（修正副本数，匹配1个BE节点）
CREATE TABLE dim_page_info (
    page_id INT COMMENT '页面ID，与ODS层page_id关联',
    page_name STRING COMMENT '页面名称（如“首页-2025版”“年货节承接页”）',
    page_type STRING COMMENT '页面类型（首页/自定义承接页/商品详情页）',
    page_url STRING COMMENT '页面访问URL'
)
DUPLICATE KEY(page_id)
DISTRIBUTED BY HASH(page_id) BUCKETS 1
PROPERTIES (
    "storage_type" = "COLUMN",
    "replication_num" = "1"  -- 明确指定副本数为1，匹配可用BE节点
);

-- 插入页面维度数据（覆盖ODS层可能的page_id范围）
INSERT INTO dim_page_info VALUES
(1, '官网首页', '首页', 'https://example.com/index'),
(2, '1月促销活动页', '自定义承接页', 'https://example.com/promo/jan'),
(3, '运动鞋A详情页', '商品详情页', 'https://example.com/item/1001'),
-- 补充至50条数据（与ODS层page_id=1-50匹配）
(50, '连衣裙B详情页', '商品详情页', 'https://example.com/item/1050');


-- ====================================2.用户维度表  dim_user_info======================================================================================

-- 2.用户维度表
CREATE TABLE dim_user_info (
    user_id BIGINT COMMENT '用户ID，与ODS层user_id关联',
    user_tag STRING COMMENT '用户标签（新用户/老用户）',
    register_date DATE COMMENT '注册日期',
    user_level TINYINT COMMENT '用户等级'
)
DUPLICATE KEY(user_id)
DISTRIBUTED BY HASH(user_id) BUCKETS 1
PROPERTIES (
    "storage_type" = "COLUMN",
    "replication_num" = "1"
);


INSERT INTO dim_user_info VALUES
(10001, ' 新用户 ', '2025-01-01', 1),
(10002, ' 老用户 ', '2024-11-15', 3),
(10003, ' 新用户 ', '2025-01-10', 1),
(10004, ' 老用户 ', '2024-09-20', 4),
(10005, ' 新用户 ', '2025-01-15', 1),
(10006, ' 老用户 ', '2024-08-05', 5),
(10007, ' 新用户 ', '2025-01-20', 1),
(10008, ' 老用户 ', '2024-10-12', 2),
(10009, ' 新用户 ', '2025-01-25', 1),
(10010, ' 老用户 ', '2024-07-30', 3),
-- 补充至 50000 条数据（与 ODS 层 user_id 范围匹配）
(60000, ' 老用户 ', '2024-05-08', 5);



-- ==================================3.时间维度表  dim_time_info========================================================================================



-- 3.时间维度表（已修正副本数）
CREATE TABLE dim_time_info (
    stat_date DATE COMMENT '统计日期',
    week_num TINYINT COMMENT '周序号（1-4）',
    is_weekend TINYINT COMMENT '是否周末（1=是）',
    is_promotion TINYINT COMMENT '是否促销日（1=是）'
)
DUPLICATE KEY(stat_date)
DISTRIBUTED BY HASH(stat_date) BUCKETS 1
PROPERTIES (
    "storage_type" = "COLUMN",
    "replication_num" = "1"  -- 匹配1个BE节点
);

-- 插入2025年1月时间维度数据（完全手动生成，不依赖numbers()）
INSERT INTO dim_time_info VALUES
('2025-01-01', 1, 1, 0),  -- 第1周，周三（非周末）
('2025-01-02', 1, 0, 0),
('2025-01-03', 1, 0, 0),
('2025-01-04', 1, 0, 0),
('2025-01-05', 1, 0, 1),  -- 促销日
('2025-01-06', 1, 0, 0),
('2025-01-07', 1, 1, 0),  -- 周末
('2025-01-08', 2, 1, 0),  -- 周末
('2025-01-09', 2, 0, 0),
('2025-01-10', 2, 0, 0),
('2025-01-11', 2, 0, 0),
('2025-01-12', 2, 0, 0),
('2025-01-13', 2, 0, 0),
('2025-01-14', 2, 0, 0),
('2025-01-15', 3, 1, 1),  -- 促销日，周末
('2025-01-16', 3, 1, 0),  -- 周末
('2025-01-17', 3, 0, 0),
('2025-01-18', 3, 0, 0),
('2025-01-19', 3, 0, 0),
('2025-01-20', 3, 0, 0),
('2025-01-21', 3, 0, 0),
('2025-01-22', 3, 0, 0),
('2025-01-23', 4, 1, 0),  -- 周末
('2025-01-24', 4, 1, 0),  -- 周末
('2025-01-25', 4, 0, 1),  -- 促销日
('2025-01-26', 4, 0, 0),
('2025-01-27', 4, 0, 0),
('2025-01-28', 4, 0, 0),
('2025-01-29', 4, 0, 0),
('2025-01-30', 4, 0, 0),
('2025-01-31', 5, 0, 0);  -- 延伸到第5周


















