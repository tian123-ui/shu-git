-- �Զ����ɵ�Hive DDL�ű�
-- Դ��: tms.transport_task_detail
use tms;
-- Ŀ���: ods_transport_task_detail
-- ����ʱ��: 2025-07-21 21:23:30

CREATE TABLE IF NOT EXISTS ods_transport_task_detail (
    id INT,
    transport_task_id INT,
    order_id INT,
    sorter_emp_id INT,
    create_time TIMESTAMP,
    update_time TIMESTAMP,
    is_deleted STRING
)
PARTITIONED BY (ds STRING)
LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/tms/ods/ods_transport_task_detail'

    TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'external.table.purge' = 'true'
    );
    