-- �Զ����ɵ�Hive DDL�ű�
-- Դ��: tms.user_address
use tms;
-- Ŀ���: ods_user_address
-- ����ʱ��: 2025-07-21 21:23:30

CREATE TABLE IF NOT EXISTS ods_user_address (
    id INT,
    user_id INT,
    phone STRING,
    province_id INT,
    city_id INT,
    district_id INT,
    complex_id INT,
    address STRING,
    is_default TINYINT,
    create_time TIMESTAMP,
    update_time TIMESTAMP,
    is_deleted STRING
)
PARTITIONED BY (ds STRING)
LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/tms/ods/ods_user_address'

    TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'external.table.purge' = 'true'
    );
    