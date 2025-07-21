-- 自动生成的Hive DDL脚本
-- 源表: tms.base_region_info
use tms;
-- 目标表: ods_base_region_info
-- 生成时间: 2025-07-21 21:23:29

CREATE TABLE IF NOT EXISTS ods_base_region_info (
    id INT,
    parent_id INT,
    name STRING,
    dict_code STRING,
    short_name STRING,
    create_time TIMESTAMP,
    update_time TIMESTAMP,
    is_deleted TINYINT
)
PARTITIONED BY (ds STRING)
LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/tms/ods/ods_base_region_info'

    TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'external.table.purge' = 'true'
    );
    