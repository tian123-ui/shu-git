-- �Զ����ɵ�Hive DDL�ű�
-- Դ��: tms.base_organ
use tms;
-- Ŀ���: ods_base_organ
-- ����ʱ��: 2025-07-21 21:23:29

CREATE TABLE IF NOT EXISTS ods_base_organ (
    id INT,
    org_name STRING,
    org_level INT,
    region_id INT,
    org_parent_id INT,
    points STRING,
    create_time TIMESTAMP,
    update_time TIMESTAMP,
    is_deleted STRING
)
PARTITIONED BY (ds STRING)
LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/tms/ods/ods_base_organ'

    TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'external.table.purge' = 'true'
    );
    