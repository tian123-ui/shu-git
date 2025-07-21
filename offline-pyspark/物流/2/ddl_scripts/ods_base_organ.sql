-- 自动生成的Hive DDL脚本
-- 源表: tms.base_organ
use tms;
-- 目标表: ods_base_organ
-- 生成时间: 2025-07-21 21:23:29

CREATE TABLE IF NOT EXISTS ods_base_organ (
    id INT,
    org_name STRING,
    org_level INT,
    region_id INT,
    org_parent_id INT,
    points STRING,
    create_time TIMESTAMP,
    update_time TIMESTAMP,
    is_deleted STRING
)
PARTITIONED BY (ds STRING)
LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/tms/ods/ods_base_organ'

    TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'external.table.purge' = 'true'
    );
    