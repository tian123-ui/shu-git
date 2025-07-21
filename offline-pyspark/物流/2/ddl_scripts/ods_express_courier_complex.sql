-- �Զ����ɵ�Hive DDL�ű�
-- Դ��: tms.express_courier_complex
use tms;
-- Ŀ���: ods_express_courier_complex
-- ����ʱ��: 2025-07-21 21:23:29

CREATE TABLE IF NOT EXISTS ods_express_courier_complex (
    id INT,
    courier_emp_id INT,
    complex_id INT,
    create_time DATE,
    update_time DATE,
    is_deleted STRING
)
PARTITIONED BY (ds STRING)
LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/tms/ods/ods_express_courier_complex'

    TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'external.table.purge' = 'true'
    );
    