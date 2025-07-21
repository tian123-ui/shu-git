-- 自动生成的Hive DDL脚本
-- 源表: tms.transport_task
use tms;
-- 目标表: ods_transport_task
-- 生成时间: 2025-07-21 21:23:30

CREATE TABLE IF NOT EXISTS ods_transport_task (
    id INT,
    shift_id INT,
    line_id INT,
    start_org_id INT,
    start_org_name STRING,
    end_org_id INT,
    end_org_name STRING,
    status STRING,
    order_num INT,
    driver1_emp_id INT,
    driver1_name STRING,
    driver2_emp_id INT,
    driver2_name STRING,
    truck_id INT,
    truck_no STRING,
    actual_start_time DATE,
    actual_end_time DATE,
    actual_distance DECIMAL(16,2),
    create_time TIMESTAMP,
    update_time TIMESTAMP,
    is_deleted STRING
)
PARTITIONED BY (ds STRING)
LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/tms/ods/ods_transport_task'

    TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'external.table.purge' = 'true'
    );
    