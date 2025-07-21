-- 自动生成的Hive DDL脚本
-- 源表: tms.order_trace_log
use tms;
-- 目标表: ods_order_trace_log
-- 生成时间: 2025-07-21 21:23:30

CREATE TABLE IF NOT EXISTS ods_order_trace_log (
    id INT,
    order_id INT,
    trace_desc STRING,
    create_time TIMESTAMP,
    cur_task_id INT,
    task_type STRING,
    charge_emp_id INT,
    remark STRING,
    is_rollback STRING,
    update_time TIMESTAMP,
    is_deleted STRING
)
PARTITIONED BY (ds STRING)
LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/tms/ods/ods_order_trace_log'

    TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'external.table.purge' = 'true'
    );
    