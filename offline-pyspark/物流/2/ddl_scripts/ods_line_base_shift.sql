-- 自动生成的Hive DDL脚本
-- 源表: tms.line_base_shift
use tms;
-- 目标表: ods_line_base_shift
-- 生成时间: 2025-07-21 21:23:29

CREATE TABLE IF NOT EXISTS ods_line_base_shift (
    id INT,
    line_id INT,
    start_time STRING,
    driver1_emp_id INT,
    driver2_emp_id INT,
    truck_id INT,
    pair_shift_id INT,
    is_enabled STRING,
    create_time TIMESTAMP,
    update_time TIMESTAMP,
    is_deleted STRING
)
PARTITIONED BY (ds STRING)
LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/tms/ods/ods_line_base_shift'

    TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'external.table.purge' = 'true'
    );
    