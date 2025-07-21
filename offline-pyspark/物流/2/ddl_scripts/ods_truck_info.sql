-- 自动生成的Hive DDL脚本
-- 源表: tms.truck_info
use tms;
-- 目标表: ods_truck_info
-- 生成时间: 2025-07-21 21:23:30

CREATE TABLE IF NOT EXISTS ods_truck_info (
    id INT,
    team_id INT,
    truck_no STRING,
    truck_model_id STRING,
    device_gps_id STRING,
    engine_no STRING,
    license_registration_date DATE,
    license_last_check_date DATE,
    license_expire_date DATE,
    picture_url STRING,
    is_enabled TINYINT,
    create_time TIMESTAMP,
    update_time TIMESTAMP,
    is_deleted STRING
)
PARTITIONED BY (ds STRING)
LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/tms/ods/ods_truck_info'

    TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'external.table.purge' = 'true'
    );
    