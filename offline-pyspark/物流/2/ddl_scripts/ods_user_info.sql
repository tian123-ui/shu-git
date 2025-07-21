-- �Զ����ɵ�Hive DDL�ű�
-- Դ��: tms.user_info
use tms;
-- Ŀ���: ods_user_info
-- ����ʱ��: 2025-07-21 21:23:30

CREATE TABLE IF NOT EXISTS ods_user_info (
    id INT,
    login_name STRING,
    nick_name STRING,
    passwd STRING,
    real_name STRING,
    phone_num STRING,
    email STRING,
    head_img STRING,
    user_level STRING,
    birthday DATE,
    gender STRING,
    create_time DATE,
    update_time DATE,
    is_deleted STRING
)
PARTITIONED BY (ds STRING)
LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/tms/ods/ods_user_info'

    TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'external.table.purge' = 'true'
    );
    