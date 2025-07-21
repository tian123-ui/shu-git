-- 自动生成的Hive DDL脚本
-- 源表: tms.line_base_info
use tms;
-- 目标表: ods_line_base_info
-- 生成时间: 2025-07-21 21:23:29

CREATE TABLE IF NOT EXISTS ods_line_base_info (
    id INT,
    name STRING,
    line_no STRING,
    line_level STRING,
    org_id INT,
    transport_line_type_id STRING,
    start_org_id INT,
    start_org_name STRING,
    end_org_id INT,
    end_org_name STRING,
    pair_line_id INT,
    distance DECIMAL(10,2),
    cost DECIMAL(10,2),
    estimated_time INT,
    status STRING,
    create_time TIMESTAMP,
    update_time TIMESTAMP,
    is_deleted STRING
)
PARTITIONED BY (ds STRING)
LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/tms/ods/ods_line_base_info'

    TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'external.table.purge' = 'true'
    );
    