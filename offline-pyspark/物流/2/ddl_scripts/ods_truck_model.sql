-- 自动生成的Hive DDL脚本
-- 源表: tms.truck_model
use tms;
-- 目标表: ods_truck_model
-- 生成时间: 2025-07-21 21:23:30

CREATE TABLE IF NOT EXISTS ods_truck_model (
    id INT,
    model_name STRING,
    model_type STRING,
    model_no STRING,
    brand STRING,
    truck_weight DECIMAL(10,2),
    load_weight DECIMAL(10,2),
    total_weight DECIMAL(10,2),
    eev STRING,
    boxcar_len DECIMAL(10,2),
    boxcar_wd DECIMAL(10,2),
    boxcar_hg DECIMAL(10,2),
    max_speed INT,
    oil_vol INT,
    create_time DATE,
    update_time DATE,
    is_deleted STRING
)
PARTITIONED BY (ds STRING)
LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/tms/ods/ods_truck_model'

    TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'external.table.purge' = 'true'
    );
    