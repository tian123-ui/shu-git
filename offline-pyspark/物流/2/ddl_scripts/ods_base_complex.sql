-- �Զ����ɵ�Hive DDL�ű�
-- Դ��: tms.base_complex
use tms;
-- Ŀ���: ods_base_complex
-- ����ʱ��: 2025-07-21 21:23:29

CREATE TABLE IF NOT EXISTS ods_base_complex (
    id INT,
    complex_name STRING,
    province_id INT,
    city_id INT,
    district_id INT,
    district_name STRING,
    create_time DATE,
    update_time DATE,
    is_deleted STRING
)
PARTITIONED BY (ds STRING)
LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/tms/ods/ods_base_complex'

    TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'external.table.purge' = 'true'
    );
    