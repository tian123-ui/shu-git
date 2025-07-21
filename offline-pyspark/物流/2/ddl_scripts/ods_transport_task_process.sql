-- 自动生成的Hive DDL脚本
-- 源表: tms.transport_task_process
use tms;
-- 目标表: ods_transport_task_process
-- 生成时间: 2025-07-21 21:23:30

CREATE TABLE IF NOT EXISTS ods_transport_task_process (
    id INT,
    transport_task_id INT,
    cur_distance DECIMAL(16,2),
    line_distance DECIMAL(16,2),
    create_time DATE,
    update_time DATE,
    is_deleted STRING
)
PARTITIONED BY (ds STRING)
LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/tms/ods/ods_transport_task_process'

    TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'external.table.purge' = 'true'
    );
    