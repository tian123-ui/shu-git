-- �Զ����ɵ�Hive DDL�ű�
-- Դ��: tms.order_org_bound
use tms;
-- Ŀ���: ods_order_org_bound
-- ����ʱ��: 2025-07-21 21:23:30

CREATE TABLE IF NOT EXISTS ods_order_org_bound (
    id INT,
    order_id INT,
    org_id INT,
    status STRING,
    inbound_time DATE,
    inbound_emp_id INT,
    sort_time DATE,
    sorter_emp_id INT,
    outbound_time DATE,
    outbound_emp_id INT,
    create_time DATE,
    update_time DATE,
    is_deleted STRING
)
PARTITIONED BY (ds STRING)
LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/tms/ods/ods_order_org_bound'

    TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'external.table.purge' = 'true'
    );
    