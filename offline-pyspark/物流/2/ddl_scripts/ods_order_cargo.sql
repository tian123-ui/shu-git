-- �Զ����ɵ�Hive DDL�ű�
-- Դ��: tms.order_cargo
use tms;
-- Ŀ���: ods_order_cargo
-- ����ʱ��: 2025-07-21 21:23:29

CREATE TABLE IF NOT EXISTS ods_order_cargo (
    id INT,
    order_id STRING,
    cargo_type STRING,
    volume_length INT,
    volume_width INT,
    volume_height INT,
    weight DECIMAL(16,2),
    remark STRING,
    create_time TIMESTAMP,
    update_time TIMESTAMP,
    is_deleted STRING
)
PARTITIONED BY (ds STRING)
LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/tms/ods/ods_order_cargo'

    TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'external.table.purge' = 'true'
    );
    