-- 自动生成的Hive DDL脚本
-- 源表: tms.order_info
use tms;
-- 目标表: ods_order_info
-- 生成时间: 2025-07-21 21:23:30

CREATE TABLE IF NOT EXISTS ods_order_info (
    id INT,
    order_no STRING,
    status STRING,
    collect_type STRING,
    user_id INT,
    receiver_complex_id INT,
    receiver_province_id STRING,
    receiver_city_id STRING,
    receiver_district_id STRING,
    receiver_address STRING,
    receiver_name STRING,
    receiver_phone STRING,
    receive_location STRING,
    sender_complex_id INT,
    sender_province_id STRING,
    sender_city_id STRING,
    sender_district_id STRING,
    sender_address STRING,
    sender_name STRING,
    sender_phone STRING,
    send_location STRING,
    payment_type STRING,
    cargo_num INT,
    amount DECIMAL(32,2),
    estimate_arrive_time DATE,
    distance DECIMAL(10,2),
    create_time TIMESTAMP,
    update_time TIMESTAMP,
    is_deleted STRING
)
PARTITIONED BY (ds STRING)
LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/tms/ods/ods_order_info'

    TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'external.table.purge' = 'true'
    );
    