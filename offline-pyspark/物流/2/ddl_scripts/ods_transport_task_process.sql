-- �Զ����ɵ�Hive DDL�ű�
-- Դ��: tms.transport_task_process
use tms;
-- Ŀ���: ods_transport_task_process
-- ����ʱ��: 2025-07-21 21:23:30

CREATE TABLE IF NOT EXISTS ods_transport_task_process (
    id INT,
    transport_task_id INT,
    cur_distance DECIMAL(16,2),
    line_distance DECIMAL(16,2),
    create_time DATE,
    update_time DATE,
    is_deleted STRING
)
PARTITIONED BY (ds STRING)
LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/tms/ods/ods_transport_task_process'

    TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'external.table.purge' = 'true'
    );
    