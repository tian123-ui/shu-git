-- �Զ����ɵ�Hive DDL�ű�
-- Դ��: tms.employee_info
use tms;
-- Ŀ���: ods_employee_info
-- ����ʱ��: 2025-07-21 21:23:29

CREATE TABLE IF NOT EXISTS ods_employee_info (
    id INT,
    username STRING,
    password STRING,
    real_name STRING,
    id_card STRING,
    phone STRING,
    birthday STRING,
    gender STRING,
    address STRING,
    employment_date STRING,
    graduation_date STRING,
    education STRING,
    position_type STRING,
    create_time TIMESTAMP,
    update_time TIMESTAMP,
    is_deleted STRING
)
PARTITIONED BY (ds STRING)
LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/tms/ods/ods_employee_info'

    TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'external.table.purge' = 'true'
    );
    