-- 自动生成的Hive DDL脚本
-- 源表: tms.express_courier_complex
use tms;
-- 目标表: ods_express_courier_complex
-- 生成时间: 2025-07-21 21:23:29

CREATE TABLE IF NOT EXISTS ods_express_courier_complex (
    id INT,
    courier_emp_id INT,
    complex_id INT,
    create_time DATE,
    update_time DATE,
    is_deleted STRING
)
PARTITIONED BY (ds STRING)
LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/tms/ods/ods_express_courier_complex'

    TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'external.table.purge' = 'true'
    );
    