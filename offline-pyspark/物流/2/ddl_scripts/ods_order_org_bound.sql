-- 自动生成的Hive DDL脚本
-- 源表: tms.order_org_bound
use tms;
-- 目标表: ods_order_org_bound
-- 生成时间: 2025-07-21 21:23:30

CREATE TABLE IF NOT EXISTS ods_order_org_bound (
    id INT,
    order_id INT,
    org_id INT,
    status STRING,
    inbound_time DATE,
    inbound_emp_id INT,
    sort_time DATE,
    sorter_emp_id INT,
    outbound_time DATE,
    outbound_emp_id INT,
    create_time DATE,
    update_time DATE,
    is_deleted STRING
)
PARTITIONED BY (ds STRING)
LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/tms/ods/ods_order_org_bound'

    TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'external.table.purge' = 'true'
    );
    