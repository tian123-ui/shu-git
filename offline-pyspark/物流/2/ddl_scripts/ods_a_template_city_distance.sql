-- �Զ����ɵ�Hive DDL�ű�
-- Դ��: tms.a_template_city_distance
use tms;
-- Ŀ���: ods_a_template_city_distance
-- ����ʱ��: 2025-07-21 21:23:29

CREATE TABLE IF NOT EXISTS ods_a_template_city_distance (
    id INT,
    city_no1 INT,
    city_no2 INT,
    distance INT,
    remark STRING
)
PARTITIONED BY (ds STRING)
LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/tms/ods/ods_a_template_city_distance'

    TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'external.table.purge' = 'true'
    );
    