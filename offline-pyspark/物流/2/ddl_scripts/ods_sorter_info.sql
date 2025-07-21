-- �Զ����ɵ�Hive DDL�ű�
-- Դ��: tms.sorter_info
use tms;
-- Ŀ���: ods_sorter_info
-- ����ʱ��: 2025-07-21 21:23:30

CREATE TABLE IF NOT EXISTS ods_sorter_info (
    id INT,
    emp_id INT,
    org_id INT,
    create_time TIMESTAMP,
    update_time TIMESTAMP,
    is_deleted STRING
)
PARTITIONED BY (ds STRING)
LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/tms/ods/ods_sorter_info'

    TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'external.table.purge' = 'true'
    );
    