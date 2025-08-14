
 
CREATE DATABASE IF NOT EXISTS gon09
CHARACTER SET utf8mb4
COLLATE utf8mb4_unicode_ci;


use gon09;

-- 声明脚本用途：基于流量主题店内路径看板设计的数仓分层表结构及数据
-- 引用文件：大数据-电商数仓-09-流量主题店内路径看板V1.2-20250126.pdf

-- 一、ODS层（操作数据存储层）
-- 1. 创建用户行为日志表
CREATE TABLE IF NOT EXISTS `ods_page_behavior` (
  `visitor_id` varchar(50) NOT NULL COMMENT '访客ID',
  `visit_time` datetime NOT NULL COMMENT '访问时间',
  `page_name` varchar(100) NOT NULL COMMENT '页面名称',
  `page_type` enum('店铺页','商品详情页','店铺其他页') NOT NULL COMMENT '页面类型',
  `source_page` varchar(100) DEFAULT NULL COMMENT '来源页面（NULL表示直接入店）',
  `target_page` varchar(100) DEFAULT NULL COMMENT '去向页面（NULL表示退出）',
  `stay_duration` int NOT NULL COMMENT '停留时长（秒）',
  `terminal_type` enum('无线端','PC端') NOT NULL COMMENT '终端类型',
  `is_order` tinyint(1) NOT NULL DEFAULT 0 COMMENT '是否下单（1=是，0=否）',
  `order_buyer_id` varchar(50) DEFAULT NULL COMMENT '下单买家ID（is_order=1时有效）',
  PRIMARY KEY (`visitor_id`, `visit_time`) COMMENT '复合主键确保唯一性'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT 'ODS层用户行为原始日志表（引用文件：大数据-电商数仓-09-流量主题店内路径看板V1.2-20250126.pdf）';

-- 2. 创建存储过程生成1000条ODS层模拟数据
DELIMITER $$
DROP PROCEDURE IF EXISTS generate_ods_data$$
CREATE PROCEDURE generate_ods_data()
BEGIN
  DECLARE i INT DEFAULT 1;
  DECLARE visitor_id VARCHAR(50);
  DECLARE visit_time DATETIME;
  DECLARE page_name VARCHAR(100);
  DECLARE page_type ENUM('店铺页','商品详情页','店铺其他页');
  DECLARE source_page VARCHAR(100);
  DECLARE target_page VARCHAR(100);
  DECLARE stay_duration INT;
  DECLARE terminal_type ENUM('无线端','PC端');
  DECLARE is_order TINYINT(1);
  DECLARE order_buyer_id VARCHAR(50);
  
  -- 循环生成1000条数据
  WHILE i <= 1000 DO
    -- 生成访客ID
    SET visitor_id = CONCAT('VISITOR_', LPAD(i, 4, '0'));
    
    -- 生成访问时间（2025-01-01至2025-01-31随机）
    SET visit_time = DATE_ADD('2025-01-01', INTERVAL FLOOR(RAND()*31) DAY);
    SET visit_time = DATE_ADD(visit_time, INTERVAL FLOOR(RAND()*24) HOUR);
    SET visit_time = DATE_ADD(visit_time, INTERVAL FLOOR(RAND()*60) MINUTE);
    
    -- 随机页面类型（匹配引用文件中页面分类体系）
    SET page_type = ELT(FLOOR(RAND()*3)+1, '店铺页', '商品详情页', '店铺其他页');
    
    -- 基于页面类型生成页面名称
    IF page_type = '店铺页' THEN
      SET page_name = ELT(FLOOR(RAND()*5)+1, '首页', '活动页', '分类页', '宝贝页', '新品页');
    ELSEIF page_type = '商品详情页' THEN
      SET page_name = CONCAT('商品详情页_', FLOOR(RAND()*100)+1);
    ELSE
      SET page_name = ELT(FLOOR(RAND()*2)+1, '订阅页', '直播页');
    END IF;
    
    -- 随机来源页面（30%概率无来源）
    IF RAND() > 0.3 THEN
      SET source_page = CASE 
        WHEN RAND() > 0.5 THEN '首页' 
        ELSE CONCAT('商品详情页_', FLOOR(RAND()*100)+1) 
      END;
    ELSE
      SET source_page = NULL;
    END IF;
    
    -- 随机去向页面（20%概率退出）
    IF RAND() > 0.2 THEN
      SET target_page = CASE 
        WHEN RAND() > 0.6 THEN '购物车' 
        WHEN RAND() > 0.3 THEN '下单页' 
        ELSE '首页' 
      END;
    ELSE
      SET target_page = NULL;
    END IF;
    
    -- 停留时长（10-300秒）
    SET stay_duration = FLOOR(RAND()*290) + 10;
    
    -- 终端类型（70%无线端，30%PC端，匹配引用文件中多终端覆盖要求）
    SET terminal_type = IF(RAND() > 0.3, '无线端', 'PC端');
    
    -- 下单行为（5%概率下单）
    SET is_order = IF(RAND() > 0.95, 1, 0);
    SET order_buyer_id = IF(is_order = 1, CONCAT('BUYER_', LPAD(FLOOR(RAND()*1000), 4, '0')), NULL);
    
    -- 插入数据
    INSERT INTO `ods_page_behavior` VALUES (
      visitor_id, visit_time, page_name, page_type, 
      source_page, target_page, stay_duration, 
      terminal_type, is_order, order_buyer_id
    );
    
    SET i = i + 1;
  END WHILE;
END $$
DELIMITER ;

-- 执行存储过程生成1000条数据
CALL generate_ods_data();

-- 二、DIM层（维度表层）
-- 1. 页面维度表（匹配引用文件中页面分类体系）
CREATE TABLE IF NOT EXISTS `dim_page` (
  `page_id` int NOT NULL AUTO_INCREMENT COMMENT '页面ID',
  `page_type` enum('店铺页','商品详情页','店铺其他页') NOT NULL COMMENT '页面类型',
  `page_subtype` varchar(50) NOT NULL COMMENT '页面细分类型',
  `page_name` varchar(100) NOT NULL COMMENT '页面名称',
  PRIMARY KEY (`page_id`),
  UNIQUE KEY `uk_page_name` (`page_name`) COMMENT '页面名称唯一'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '页面维度表（引用文件：大数据-电商数仓-09-流量主题店内路径看板V1.2-20250126.pdf）';

-- 插入页面维度数据
INSERT INTO `dim_page` (`page_type`, `page_subtype`, `page_name`) VALUES
('店铺页', '首页', '首页'),
('店铺页', '活动页', '活动页'),
('店铺页', '分类页', '分类页'),
('店铺页', '宝贝页', '宝贝页'),
('店铺页', '新品页', '新品页'),
('商品详情页', '基础详情页', '商品详情页_1'),
('商品详情页', '基础详情页', '商品详情页_2'),
('商品详情页', '基础详情页', '商品详情页_3'),
('商品详情页', '基础详情页', '商品详情页_4'),
('商品详情页', '基础详情页', '商品详情页_5'),
('店铺其他页', '订阅页', '订阅页'),
('店铺其他页', '直播页', '直播页');

-- 2. 时间维度表（支持引用文件中的多时间粒度查询）
CREATE TABLE IF NOT EXISTS `dim_time` (
  `time_id` int NOT NULL AUTO_INCREMENT COMMENT '时间ID',
  `time_granularity` enum('日','7天','30天','月') NOT NULL COMMENT '时间粒度',
  `date_code` varchar(20) NOT NULL COMMENT '日期编码',
  `start_date` date NOT NULL COMMENT '开始日期',
  `end_date` date NOT NULL COMMENT '结束日期',
  PRIMARY KEY (`time_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '时间维度表（引用文件：大数据-电商数仓-09-流量主题店内路径看板V1.2-20250126.pdf）';

-- 插入时间维度数据
INSERT INTO `dim_time` (`time_granularity`, `date_code`, `start_date`, `end_date`) VALUES
('日', '20250101', '2025-01-01', '2025-01-01'),
('日', '20250102', '2025-01-02', '2025-01-02'),
('日', '20250103', '2025-01-03', '2025-01-03'),
('7天', '202501W1', '2025-01-01', '2025-01-07'),
('30天', '202501', '2025-01-01', '2025-01-31'),
('月', '202501', '2025-01-01', '2025-01-31');

-- 3. 设备维度表（区分终端类型）
CREATE TABLE IF NOT EXISTS `dim_device` (
  `device_id` int NOT NULL AUTO_INCREMENT COMMENT '设备ID',
  `terminal_type` enum('无线端','PC端') NOT NULL COMMENT '终端类型',
  `device_identifier` varchar(50) NOT NULL COMMENT '设备标识（如系统类型）',
  PRIMARY KEY (`device_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '设备维度表（引用文件：大数据-电商数仓-09-流量主题店内路径看板V1.2-20250126.pdf）';

-- 插入设备维度数据
INSERT INTO `dim_device` (`terminal_type`, `device_identifier`) VALUES
('无线端', 'iOS'),
('无线端', 'Android'),
('PC端', 'Windows'),
('PC端', 'macOS');

-- 三、DWD层（明细数据层）
-- 1. 用户页面访问明细事实表（清洗后明细数据）
CREATE TABLE IF NOT EXISTS `dwd_user_page_visit_detail` (
  `id` bigint NOT NULL AUTO_INCREMENT COMMENT '明细ID',
  `visitor_id` varchar(50) NOT NULL COMMENT '访客ID',
  `time_id` int NOT NULL COMMENT '访问时间ID（关联dim_time）',
  `page_id` int NOT NULL COMMENT '页面ID（关联dim_page）',
  `page_type` enum('店铺页','商品详情页','店铺其他页') NOT NULL COMMENT '页面类型',
  `source_page_id` int DEFAULT NULL COMMENT '来源页面ID（关联dim_page）',
  `target_page_id` int DEFAULT NULL COMMENT '去向页面ID（关联dim_page）',
  `stay_duration` int NOT NULL COMMENT '停留时长（秒）',
  `device_id` int NOT NULL COMMENT '设备ID（关联dim_device）',
  `is_order` tinyint(1) NOT NULL DEFAULT 0 COMMENT '是否下单',
  `order_buyer_id` varchar(50) DEFAULT NULL COMMENT '下单买家ID',
  PRIMARY KEY (`id`),
  KEY `idx_visitor_time` (`visitor_id`, `time_id`),
  KEY `fk_page_id` (`page_id`),
  KEY `fk_time_id` (`time_id`),
  KEY `fk_device_id` (`device_id`),
  CONSTRAINT `fk_dwd_page` FOREIGN KEY (`page_id`) REFERENCES `dim_page` (`page_id`),
  CONSTRAINT `fk_dwd_time` FOREIGN KEY (`time_id`) REFERENCES `dim_time` (`time_id`),
  CONSTRAINT `fk_dwd_device` FOREIGN KEY (`device_id`) REFERENCES `dim_device` (`device_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT 'DWD层用户页面访问明细（引用文件：大数据-电商数仓-09-流量主题店内路径看板V1.2-20250126.pdf）';

-- 插入DWD层示例数据（从ODS层关联维度转换）
INSERT INTO `dwd_user_page_visit_detail` (
  `visitor_id`, `time_id`, `page_id`, `page_type`, 
  `source_page_id`, `target_page_id`, `stay_duration`, 
  `device_id`, `is_order`, `order_buyer_id`
) VALUES
-- 示例1：无线端访问首页，来源为空，去向活动页
('VISITOR_0001', 1, 1, '店铺页', NULL, 2, 45, 1, 0, NULL),
-- 示例2：PC端访问商品详情页，来源分类页，去向下单页（下单）
('VISITOR_0002', 1, 6, '商品详情页', 3, NULL, 120, 3, 1, 'BUYER_0123'),
-- 示例3：无线端访问直播页，来源首页，去向商品详情页
('VISITOR_0003', 1, 12, '店铺其他页', 1, 7, 300, 2, 0, NULL),
-- 示例4：无线端访问活动页，来源新品页，去向退出
('VISITOR_0004', 1, 2, '店铺页', 5, NULL, 20, 1, 0, NULL),
-- 示例5：PC端访问商品详情页，来源搜索页（未在dim_page中定义，来源ID为NULL），去向下单页
('VISITOR_0005', 1, 8, '商品详情页', NULL, NULL, 90, 3, 1, 'BUYER_0234');

-- 四、DWS层（汇总数据层）
-- 1. 页面访问指标汇总表
CREATE TABLE IF NOT EXISTS `dws_page_visit_stats` (
  `id` int NOT NULL AUTO_INCREMENT COMMENT '汇总ID',
  `time_granularity` enum('日','7天','30天','月') NOT NULL COMMENT '时间粒度',
  `page_type` enum('店铺页','商品详情页','店铺其他页') NOT NULL COMMENT '页面类型',
  `terminal_type` enum('无线端','PC端') NOT NULL COMMENT '终端类型',
  `visitor_count` int NOT NULL COMMENT '访客数',
  `page_view` int NOT NULL COMMENT '浏览量',
  `avg_stay_duration` decimal(10,2) NOT NULL COMMENT '平均停留时长（秒）',
  `order_buyer_count` int NOT NULL DEFAULT 0 COMMENT '下单买家数',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_stats` (`time_granularity`, `page_type`, `terminal_type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT 'DWS层页面访问指标汇总（引用文件：大数据-电商数仓-09-流量主题店内路径看板V1.2-20250126.pdf）';

-- 插入DWS层汇总数据
INSERT INTO `dws_page_visit_stats` (
  `time_granularity`, `page_type`, `terminal_type`, 
  `visitor_count`, `page_view`, `avg_stay_duration`, `order_buyer_count`
) VALUES
('日', '店铺页', '无线端', 350, 420, 68.50, 12),
('日', '商品详情页', 'PC端', 180, 210, 156.30, 28),
('日', '店铺其他页', '无线端', 90, 95, 210.70, 5),
('7天', '店铺页', '无线端', 2200, 2600, 72.30, 85),
('7天', '商品详情页', 'PC端', 1100, 1300, 145.80, 170);

-- 2. 店内路径流转汇总表（保留路径不重复、多向性特性）
CREATE TABLE IF NOT EXISTS `dws_instore_path_flow_stats` (
  `id` int NOT NULL AUTO_INCREMENT COMMENT '流转ID',
  `time_granularity` enum('日','7天','30天','月') NOT NULL COMMENT '时间粒度',
  `terminal_type` enum('无线端','PC端') NOT NULL COMMENT '终端类型',
  `source_page_id` int DEFAULT NULL COMMENT '来源页面ID',
  `target_page_id` int DEFAULT NULL COMMENT '去向页面ID',
  `visitor_count` int NOT NULL COMMENT '访客数',
  PRIMARY KEY (`id`),
  KEY `idx_flow` (`time_granularity`, `terminal_type`, `source_page_id`, `target_page_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT 'DWS层店内路径流转汇总（引用文件：大数据-电商数仓-09-流量主题店内路径看板V1.2-20250126.pdf）';

-- 插入路径流转数据
INSERT INTO `dws_instore_path_flow_stats` (
  `time_granularity`, `terminal_type`, `source_page_id`, `target_page_id`, `visitor_count`
) VALUES
('日', '无线端', 1, 2, 120), -- 首页→活动页
('日', '无线端', 1, 12, 85), -- 首页→直播页
('日', 'PC端', 3, 6, 90), -- 分类页→商品详情页_1
('日', '无线端', 12, 7, 60), -- 直播页→商品详情页_2
('日', 'PC端', 6, NULL, 30); -- 商品详情页_1→退出

-- 3. PC端流量入口汇总表
CREATE TABLE IF NOT EXISTS `dws_pc_traffic_entry_stats` (
  `id` int NOT NULL AUTO_INCREMENT COMMENT 'ID',
  `time_granularity` enum('日','7天','30天','月') NOT NULL COMMENT '时间粒度',
  `source_page` varchar(100) NOT NULL COMMENT '来源页面',
  `visitor_count` int NOT NULL COMMENT '访客数',
  `source_proportion` decimal(5,2) NOT NULL COMMENT '来源占比（%）',
  PRIMARY KEY (`id`),
  KEY `idx_pc_source` (`time_granularity`, `source_page`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT 'DWS层PC端流量入口汇总（引用文件：大数据-电商数仓-09-流量主题店内路径看板V1.2-20250126.pdf）';

-- 插入PC端流量入口数据
INSERT INTO `dws_pc_traffic_entry_stats` (
  `time_granularity`, `source_page`, `visitor_count`, `source_proportion`
) VALUES
('日', '分类页', 90, 45.00),
('日', '搜索页', 60, 30.00),
('日', '首页', 30, 15.00),
('日', '外部链接', 20, 10.00),
('7天', '分类页', 550, 42.30),
('7天', '搜索页', 400, 30.77);

-- 五、ADS层（应用数据层）
-- 1. 无线端入店与承接指标表
CREATE TABLE IF NOT EXISTS `ads_wireless_entry_receive` (
  `id` int NOT NULL AUTO_INCREMENT COMMENT 'ID',
  `time_dimension` enum('日','7天','30天','月') NOT NULL COMMENT '时间维度',
  `entry_page_type` enum('店铺页','商品详情页','店铺其他页') NOT NULL COMMENT '进店页面类型',
  `visitor_count` int NOT NULL COMMENT '访客数',
  `order_buyer_count` int NOT NULL DEFAULT 0 COMMENT '下单买家数',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_wireless` (`time_dimension`, `entry_page_type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT 'ADS层无线端入店指标（引用文件：大数据-电商数仓-09-流量主题店内路径看板V1.2-20250126.pdf）';

-- 插入无线端入店数据
INSERT INTO `ads_wireless_entry_receive` (`time_dimension`, `entry_page_type`, `visitor_count`, `order_buyer_count`) VALUES
('日', '店铺页', 320, 10),
('日', '店铺其他页', 80, 3),
('日', '商品详情页', 50, 8),
('7天', '店铺页', 2100, 75),
('7天', '店铺其他页', 550, 22);

-- 2. 页面访问排行表
CREATE TABLE IF NOT EXISTS `ads_page_visit_rank` (
  `id` int NOT NULL AUTO_INCREMENT COMMENT 'ID',
  `time_dimension` enum('日','7天','30天','月') NOT NULL COMMENT '时间维度',
  `terminal_type` enum('无线端','PC端') NOT NULL COMMENT '终端类型',
  `page_type` enum('店铺页','商品详情页','店铺其他页') NOT NULL COMMENT '页面类型',
  `page_name` varchar(100) NOT NULL COMMENT '页面名称',
  `visitor_count` int NOT NULL COMMENT '访客数',
  `rank` int NOT NULL COMMENT '排名',
  PRIMARY KEY (`id`),
  KEY `idx_rank` (`time_dimension`, `terminal_type`, `page_type`, `rank`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT 'ADS层页面访问排行（引用文件：大数据-电商数仓-09-流量主题店内路径看板V1.2-20250126.pdf）';

-- 插入页面访问排行数据
INSERT INTO `ads_page_visit_rank` (
  `time_dimension`, `terminal_type`, `page_type`, `page_name`, `visitor_count`, `rank`
) VALUES
('日', '无线端', '店铺页', '首页', 210, 1),
('日', '无线端', '店铺页', '活动页', 180, 2),
('日', '无线端', '店铺页', '分类页', 150, 3),
('日', 'PC端', '商品详情页', '商品详情页_1', 95, 1),
('日', 'PC端', '商品详情页', '商品详情页_3', 85, 2);

-- 3. 店内路径展示表
CREATE TABLE IF NOT EXISTS `ads_instore_path_display` (
  `id` int NOT NULL AUTO_INCREMENT COMMENT 'ID',
  `time_dimension` enum('日','7天','30天','月') NOT NULL COMMENT '时间维度',
  `terminal_type` enum('无线端','PC端') NOT NULL COMMENT '终端类型',
  `page_name` varchar(100) NOT NULL COMMENT '页面名称',
  `source_page_name` varchar(100) DEFAULT NULL COMMENT '来源页面名称',
  `source_visitor_count` int NOT NULL COMMENT '来源访客数',
  `target_page_name` varchar(100) DEFAULT NULL COMMENT '去向页面名称',
  `target_visitor_count` int NOT NULL COMMENT '去向访客数',
  `source_proportion` decimal(5,2) DEFAULT NULL COMMENT '来源占比（%）',
  `target_proportion` decimal(5,2) DEFAULT NULL COMMENT '去向占比（%）',
  PRIMARY KEY (`id`),
  KEY `idx_path` (`time_dimension`, `terminal_type`, `page_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT 'ADS层店内路径展示（引用文件：大数据-电商数仓-09-流量主题店内路径看板V1.2-20250126.pdf）';

-- 插入店内路径数据
INSERT INTO `ads_instore_path_display` (
  `time_dimension`, `terminal_type`, `page_name`, `source_page_name`, 
  `source_visitor_count`, `target_page_name`, `target_visitor_count`,
  `source_proportion`, `target_proportion`
) VALUES
('日', '无线端', '首页', NULL, 0, '活动页', 120, NULL, 57.14),
('日', '无线端', '首页', NULL, 0, '直播页', 85, NULL, 40.48),
('日', '无线端', '直播页', '首页', 85, '商品详情页_2', 60, 100.00, 70.59),
('日', 'PC端', '商品详情页_1', '分类页', 90, NULL, 30, 100.00, 33.33);

-- 4. PC端流量入口TOP20表
CREATE TABLE IF NOT EXISTS `ads_pc_entry_top20` (
  `id` int NOT NULL AUTO_INCREMENT COMMENT 'ID',
  `time_dimension` enum('日','7天','30天','月') NOT NULL COMMENT '时间维度',
  `source_page` varchar(100) NOT NULL COMMENT '来源页面',
  `visitor_count` int NOT NULL COMMENT '访客数',
  `source_proportion` decimal(5,2) NOT NULL COMMENT '来源占比（%）',
  `rank` int NOT NULL COMMENT '排名',
  PRIMARY KEY (`id`),
  KEY `idx_pc_top` (`time_dimension`, `rank`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT 'ADS层PC端流量入口TOP20（引用文件：大数据-电商数仓-09-流量主题店内路径看板V1.2-20250126.pdf）';

-- 插入PC端流量入口TOP20数据（示例前5名）
INSERT INTO `ads_pc_entry_top20` (
  `time_dimension`, `source_page`, `visitor_count`, `source_proportion`, `rank`
) VALUES
('日', '分类页', 90, 45.00, 1),
('日', '搜索页', 60, 30.00, 2),
('日', '首页', 30, 15.00, 3),
('日', '外部链接', 20, 10.00, 4),
('日', '活动页', 10, 5.00, 5);

-- 脚本执行完成提示
SELECT '所有表结构创建及数据插入完成' AS result;

