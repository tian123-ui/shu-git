-- 自动生成的Hive DDL脚本
-- 源表: tms.express_task_delivery
use tms;
-- 目标表: ods_express_task_delivery
-- 生成时间: 2025-07-21 21:23:29

CREATE TABLE IF NOT EXISTS ods_express_task_delivery (
    id INT,
    order_id INT,
    status STRING,
    org_id INT,
    org_name STRING,
    courier_emp_id INT,
    courier_name STRING,
    estimated_end_time DATE,
    start_delivery_time DATE,
    delivered_time DATE,
    is_rejected STRING,
    create_time TIMESTAMP,
    update_time TIMESTAMP,
    is_deleted STRING
)
PARTITIONED BY (ds STRING)
LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/tms/ods/ods_express_task_delivery'

    TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'external.table.purge' = 'true'
    );
    