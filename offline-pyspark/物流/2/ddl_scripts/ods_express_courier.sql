-- 自动生成的Hive DDL脚本
-- 源表: tms.express_courier
use tms;
-- 目标表: ods_express_courier
-- 生成时间: 2025-07-21 21:23:29

CREATE TABLE IF NOT EXISTS ods_express_courier (
    id INT,
    emp_id INT,
    org_id INT,
    working_phone STRING,
    express_type STRING,
    create_time TIMESTAMP,
    update_time TIMESTAMP,
    is_deleted STRING
)
PARTITIONED BY (ds STRING)
LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/tms/ods/ods_express_courier'

    TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'external.table.purge' = 'true'
    );
    