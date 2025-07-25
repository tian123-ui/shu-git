-- 12.1 MySQL建库建表
-- 12.1.1 创建数据库
-- CREATE DATABASE IF NOT EXISTS tms_report DEFAULT CHARSET utf8 COLLATE utf8_general_ci;
-- 
-- 

----------------------------- 1. 物流主题 ------------------------------------------

-- 1）运单相关统计
drop table if exists ads_trans_order_stats;
create table ads_trans_order_stats(
  `dt` date NOT NULL COMMENT '统计日期',
  `recent_days` tinyint NOT NULL COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
  `receive_order_count` bigint COMMENT '接单总数',
  `receive_order_amount` decimal(16,2) COMMENT '接单金额',
  `dispatch_order_count` bigint COMMENT '发单总数',
  `dispatch_order_amount` decimal(16,2) COMMENT '发单金额',
  PRIMARY KEY (`dt`, `recent_days`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci comment '运单相关统计'
    ROW_FORMAT = DYNAMIC;


-- 2）运输综合统计
drop table if exists ads_trans_stats;
create table ads_trans_stats(
  `dt` date NOT NULL COMMENT '统计日期',
  `recent_days` tinyint NOT NULL COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
  `trans_finish_count` bigint COMMENT '完成运输次数',
  `trans_finish_distance` decimal(16,2) COMMENT '完成运输里程',
  `trans_finish_dur_sec` bigint COMMENT ' 完成运输时长，单位：秒',
  PRIMARY KEY (`dt`, `recent_days`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci comment '运输综合统计'
    ROW_FORMAT = DYNAMIC;


 -- 3）历史至今运单统计
drop table if exists ads_trans_order_stats_td;
create table ads_trans_order_stats_td(
  `dt` date NOT NULL COMMENT '统计日期',
  `bounding_order_count` bigint COMMENT '运输中运单总数',
  `bounding_order_amount` decimal(16,2) COMMENT '运输中运单金额',
  PRIMARY KEY (`dt`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci comment '历史至今运单统计'
    ROW_FORMAT = DYNAMIC;


-- 4）运单综合统计
drop table if exists ads_order_stats;
create table ads_order_stats(
  `dt` date NOT NULL COMMENT '统计日期',
  `recent_days` tinyint NOT NULL COMMENT '最近天数,1:最近天数,1:最近1天,7:最近7天,30:最近30天',
  `order_count` bigint COMMENT '下单数',
  `order_amount` decimal(16,2) COMMENT '下单金额',
  PRIMARY KEY (`dt`, `recent_days`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci comment '运单综合统计'
    ROW_FORMAT = DYNAMIC;


 -- 5）各类型货物运单统计
drop table if exists ads_order_cargo_type_stats;
create table ads_order_cargo_type_stats(
  `dt` date NOT NULL COMMENT '统计日期',
  `recent_days` tinyint NOT NULL COMMENT '最近天数,1:最近天数,1:最近1天,7:最近7天,30:最近30天',
  `cargo_type` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci COMMENT '货物类型',
  `cargo_type_name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci COMMENT '货物类型名称',
  `order_count` bigint COMMENT '下单数',
  `order_amount` decimal(16,2) COMMENT '下单金额',
  PRIMARY KEY (`dt`, `recent_days`, `cargo_type`, `cargo_type_name`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci comment '各类型货物运单统计'
    ROW_FORMAT = DYNAMIC;


-- 6）城市分析
drop table if exists ads_city_stats;
create table ads_city_stats(
  `dt` date NOT NULL COMMENT '统计日期',
  `recent_days` bigint COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
  `city_id` bigint COMMENT '城市ID',
  `city_name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci COMMENT '城市名称',
  `order_count` bigint COMMENT '下单数',
  `order_amount` decimal COMMENT '下单金额',
  `trans_finish_count` bigint COMMENT '完成运输次数',
  `trans_finish_distance` decimal(16,2) COMMENT '完成运输里程',
  `trans_finish_dur_sec` bigint COMMENT '完成运输时长，单位：秒',
  `avg_trans_finish_distance` decimal(16,2) COMMENT '平均每次运输里程',
  `avg_trans_finish_dur_sec` bigint COMMENT '平均每次运输时长，单位：秒',
  PRIMARY KEY (`dt`, `recent_days`, `city_id`, `city_name`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci comment '城市分析'
    ROW_FORMAT = DYNAMIC;


  -- 7）机构分析
drop table if exists ads_org_stats;
create table ads_org_stats(
  `dt` date NOT NULL COMMENT '统计日期',
  `recent_days` tinyint NOT NULL COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
  `org_id` bigint COMMENT '机构ID',
  `org_name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci COMMENT '机构名称',
  `order_count` bigint COMMENT '下单数',
  `order_amount` decimal COMMENT '下单金额',
  `trans_finish_count` bigint COMMENT '完成运输次数',
  `trans_finish_distance` decimal(16,2) COMMENT '完成运输里程',
  `trans_finish_dur_sec` bigint COMMENT '完成运输时长，单位：秒',
  `avg_trans_finish_distance` decimal(16,2) COMMENT '平均每次运输里程',
  `avg_trans_finish_dur_sec` bigint COMMENT '平均每次运输时长，单位：秒',
  PRIMARY KEY (`dt`, `recent_days`, `org_id`, `org_name`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci comment '机构分析'
    ROW_FORMAT = DYNAMIC;


-- 8）班次分析
drop table if exists ads_shift_stats;
create table ads_shift_stats(
  `dt` date NOT NULL COMMENT '统计日期',
  `recent_days` tinyint NOT NULL COMMENT '最近天数,7:最近7天,30:最近30天',
  `shift_id` bigint COMMENT '班次ID',
  `trans_finish_count` bigint COMMENT '完成运输次数',
  `trans_finish_distance` decimal(16,2) COMMENT '完成运输里程',
  `trans_finish_dur_sec` bigint COMMENT '完成运输时长，单位：秒',
  `trans_finish_order_count` bigint COMMENT '运输完成运单数',
  PRIMARY KEY (`dt`, `recent_days`, `shift_id`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci comment '班次分析'
    ROW_FORMAT = DYNAMIC;


  -- 9）线路分析
drop table if exists ads_line_stats;
create table ads_line_stats(
  `dt` date NOT NULL COMMENT '统计日期',
  `recent_days` tinyint NOT NULL COMMENT '最近天数,7:最近7天,30:最近30天',
  `line_id` bigint COMMENT '线路ID',
  `line_name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci COMMENT '线路名称',
  `trans_finish_count` bigint COMMENT '完成运输次数',
  `trans_finish_distance` decimal(16,2) COMMENT '完成运输里程',
  `trans_finish_dur_sec` bigint COMMENT '完成运输时长，单位：秒',
  `trans_finish_order_count` bigint COMMENT '运输完成运单数',
  PRIMARY KEY (`dt`, `recent_days`, `line_id`, `line_name`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci comment '线路分析'
    ROW_FORMAT = DYNAMIC;


-- 10）司机分析
drop table if exists ads_driver_stats;
create table ads_driver_stats(
  `dt` date NOT NULL COMMENT '统计日期',
  `recent_days` tinyint NOT NULL COMMENT '最近天数,7:最近7天,30:最近30天',
  `driver_emp_id` bigint comment '第一司机员工ID',
  `driver_name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci comment '第一司机姓名',
  `trans_finish_count` bigint COMMENT '完成运输次数',
  `trans_finish_distance` decimal(16,2) COMMENT '完成运输里程',
  `trans_finish_dur_sec` bigint COMMENT '完成运输时长，单位：秒',
  `avg_trans_finish_distance` decimal(16,2) COMMENT '平均每次运输里程',
  `avg_trans_finish_dur_sec` bigint COMMENT '平均每次运输时长，单位：秒',
  `trans_finish_late_count` bigint COMMENT '逾期次数',
  PRIMARY KEY (`dt`, `recent_days`, `driver_emp_id`, `driver_name`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci comment '司机分析'
    ROW_FORMAT = DYNAMIC;


-- 11）卡车分析
drop table if exists ads_truck_stats;
create table ads_truck_stats(
  `dt` date NOT NULL COMMENT '统计日期',
  `recent_days` tinyint NOT NULL COMMENT '最近天数,1:最近天数,1:最近1天,7:最近7天,30:最近30天',
  `truck_model_type` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci COMMENT '卡车类别编码',
  `truck_model_type_name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci COMMENT '卡车类别名称',
  `trans_finish_count` bigint COMMENT '完成运输次数',
  `trans_finish_distance` decimal(16,2) COMMENT '完成运输里程',
  `trans_finish_dur_sec` bigint COMMENT '完成运输时长，单位：秒',
  `avg_trans_finish_distance` decimal(16,2) COMMENT '平均每次运输里程',
  `avg_trans_finish_dur_sec` bigint COMMENT '平均每次运输时长，单位：秒',
  PRIMARY KEY (`dt`, `recent_days`, `truck_model_type`, `truck_model_type_name`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci comment '卡车分析'
    ROW_FORMAT = DYNAMIC;


-- 12）快递综合统计
drop table if exists ads_express_stats;
create table ads_express_stats(
  `dt` date NOT NULL COMMENT '统计日期',
  `recent_days` tinyint NOT NULL COMMENT '最近天数,1:最近天数,1:最近1天,7:最近7天,30:最近30天',
  `deliver_suc_count` bigint COMMENT '派送成功次数（订单数）',
  `sort_count` bigint COMMENT '分拣次数',
  PRIMARY KEY (`dt`, `recent_days`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci comment '快递综合统计'
    ROW_FORMAT = DYNAMIC;


  -- 13）各省份快递统计
drop table if exists ads_express_province_stats;
create table ads_express_province_stats(
  `dt` date NOT NULL COMMENT '统计日期',
  `recent_days` tinyint NOT NULL COMMENT '最近天数,1:最近天数,1:最近1天,7:最近7天,30:最近30天',
  `province_id` bigint COMMENT '省份ID',
  `province_name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci COMMENT '省份名称',
  `receive_order_count` bigint COMMENT '揽收次数',
  `receive_order_amount` decimal(16,2) COMMENT '揽收金额',
  `deliver_suc_count` bigint COMMENT '派送成功次数',
  `sort_count` bigint COMMENT '分拣次数',
  PRIMARY KEY (`dt`, `recent_days`, `province_id`, `province_name`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci comment '各省份快递统计'
    ROW_FORMAT = DYNAMIC;


  -- 14）各城市快递统计
drop table if exists ads_express_city_stats;
create table ads_express_city_stats(
  `dt` date NOT NULL COMMENT '统计日期',
  `recent_days` tinyint NOT NULL COMMENT '最近天数,1:最近天数,1:最近1天,7:最近7天,30:最近30天',
  `city_id` bigint COMMENT '城市ID',
  `city_name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci COMMENT '城市名称',
  `receive_order_count` bigint COMMENT '揽收次数',
  `receive_order_amount` decimal(16,2) COMMENT '揽收金额',
  `deliver_suc_count` bigint COMMENT '派送成功次数',
  `sort_count` bigint COMMENT '分拣次数',
  PRIMARY KEY (`dt`, `recent_days`, `city_id`, `city_name`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci comment '各城市快递统计'
    ROW_FORMAT = DYNAMIC;


  -- 15）各机构快递统计
drop table if exists ads_express_org_stats;
create table ads_express_org_stats(
  `dt` date NOT NULL COMMENT '统计日期',
  `recent_days` tinyint NOT NULL COMMENT '最近天数,1:最近天数,1:最近1天,7:最近7天,30:最近30天',
  `org_id` bigint COMMENT '机构ID',
  `org_name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci COMMENT '机构名称',
  `receive_order_count` bigint COMMENT '揽收次数',
  `receive_order_amount` decimal(16,2) COMMENT '揽收金额',
  `deliver_suc_count` bigint COMMENT '派送成功次数',
  `sort_count` bigint COMMENT '分拣次数',
  PRIMARY KEY (`dt`, `recent_days`, `org_id`, `org_name`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci comment '各机构快递统计'
    ROW_FORMAT = DYNAMIC;


