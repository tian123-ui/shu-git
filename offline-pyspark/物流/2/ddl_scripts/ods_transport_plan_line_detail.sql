-- 自动生成的Hive DDL脚本
-- 源表: tms.transport_plan_line_detail
use tms;
-- 目标表: ods_transport_plan_line_detail
-- 生成时间: 2025-07-21 21:23:30

CREATE TABLE IF NOT EXISTS ods_transport_plan_line_detail (
    id INT,
    order_id INT,
    start_org_id INT,
    end_org_id INT,
    line_base_id INT,
    create_time TIMESTAMP,
    update_time TIMESTAMP,
    is_deleted STRING
)
PARTITIONED BY (ds STRING)
LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/tms/ods/ods_transport_plan_line_detail'

    TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'external.table.purge' = 'true'
    );
    