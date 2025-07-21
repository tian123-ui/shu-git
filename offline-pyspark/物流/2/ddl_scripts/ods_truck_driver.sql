-- �Զ����ɵ�Hive DDL�ű�
-- Դ��: tms.truck_driver
use tms;
-- Ŀ���: ods_truck_driver
-- ����ʱ��: 2025-07-21 21:23:30

CREATE TABLE IF NOT EXISTS ods_truck_driver (
    id INT,
    emp_id INT,
    org_id INT,
    team_id INT,
    license_type STRING,
    init_license_date DATE,
    expire_date DATE,
    license_no STRING,
    license_picture_url STRING,
    is_enabled INT,
    create_time TIMESTAMP,
    update_time TIMESTAMP,
    is_deleted STRING
)
PARTITIONED BY (ds STRING)
LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/tms/ods/ods_truck_driver'

    TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'external.table.purge' = 'true'
    );
    