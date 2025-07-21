-- �Զ����ɵ�Hive DDL�ű�
-- Դ��: tms.base_dic
use tms;
-- Ŀ���: ods_base_dic
-- ����ʱ��: 2025-07-21 21:23:29

CREATE TABLE IF NOT EXISTS ods_base_dic (
    id INT,
    parent_id INT,
    name STRING,
    dict_code STRING,
    create_time TIMESTAMP,
    update_time TIMESTAMP,
    is_deleted TINYINT
)
PARTITIONED BY (ds STRING)
LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/tms/ods/ods_base_dic'

    TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'external.table.purge' = 'true'
    );
    