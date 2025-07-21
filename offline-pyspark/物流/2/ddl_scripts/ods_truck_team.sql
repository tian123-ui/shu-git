-- �Զ����ɵ�Hive DDL�ű�
-- Դ��: tms.truck_team
use tms;
-- Ŀ���: ods_truck_team
-- ����ʱ��: 2025-07-21 21:23:30

CREATE TABLE IF NOT EXISTS ods_truck_team (
    id INT,
    name STRING,
    team_no STRING,
    org_id INT,
    manager_emp_id INT,
    create_time TIMESTAMP,
    update_time TIMESTAMP,
    is_deleted STRING
)
PARTITIONED BY (ds STRING)
LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/tms/ods/ods_truck_team'

    TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'external.table.purge' = 'true'
    );
    