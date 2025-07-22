-- 1. 小区维度表
-- base_complex
-- base_region_info
-- express_courier_complex
drop table if exists dim_complex_full;
create external table dim_complex_full(
                                          `id` bigint comment '小区ID',
                                          `complex_name` string comment '小区名称',
                                          `courier_emp_id` string comment '负责快递员ID',
                                          `province_id` bigint comment '省份ID',
                                          `province_name` string comment '省份名称',
                                          `city_id` bigint comment '城市ID',
                                          `city_name` string comment '城市名称',
                                          `district_id` bigint comment '区（县）ID',
                                          `district_name` string comment '区（县）名称'
) comment '小区维度表'
    partitioned by (`dt` string comment '统计日期')
    stored as orc
    location '/warehouse/tms/dim/dim_complex_full'
    tblproperties('orc.compress'='snappy');

-- 2. 机构维度表
-- base_organ
-- base_region_info
drop table if exists dim_organ_full;
create external table dim_organ_full(
                                        `id` bigint COMMENT '机构ID',
                                        `org_name` string COMMENT '机构名称',
                                        `org_level` bigint COMMENT '机构等级（1为转运中心，2为转运站）',
                                        `region_id` bigint COMMENT '地区ID，1级机构为city ,2级机构为district',
                                        `region_name` string COMMENT '地区名称',
                                        `region_code` string COMMENT '地区编码（行政级别）',
                                        `org_parent_id` bigint COMMENT '父级机构ID',
                                        `org_parent_name` string COMMENT '父级机构名称'
) comment '机构维度表'
    partitioned by (`dt` string comment '统计日期')
    stored as orc
    location '/warehouse/tms/dim/dim_organ_full'
    tblproperties('orc.compress'='snappy');

-- 3. 地区维度表
-- base_region_info
drop table if exists dim_region_full;
create external table dim_region_full(
                                         `id` bigint COMMENT '地区ID',
                                         `parent_id` bigint COMMENT '上级地区ID',
                                         `name` string COMMENT '地区名称',
                                         `dict_code` string COMMENT '编码（行政级别）',
                                         `short_name` string COMMENT '简称'
) comment '地区维度表'
    partitioned by (`dt` string comment '统计日期')
    stored as orc
    location '/warehouse/tms/dim/dim_region_full'
    tblproperties('orc.compress'='snappy');

-- 4. 快递员维度表
-- express_courier
-- base_organ
-- base_dic
drop table if exists dim_express_courier_full;
create external table dim_express_courier_full(
                                                  `id` bigint COMMENT '快递员ID',
                                                  `emp_id` bigint COMMENT '员工ID',
                                                  `org_id` bigint COMMENT '所属机构ID',
                                                  `org_name` string COMMENT '机构名称',
                                                  `working_phone` string COMMENT '工作电话',
                                                  `express_type` string COMMENT '快递员类型（收货；发货）',
                                                  `express_type_name` string COMMENT '快递员类型名称'
) comment '快递员维度表'
    partitioned by (`dt` string comment '统计日期')
    stored as orc
    location '/warehouse/tms/dim/dim_express_courier_full'
    tblproperties('orc.compress'='snappy');

-- 5. 班次维度表
-- line_base_shift
-- line_base_info
-- base_dic
drop table if exists dim_shift_full;
create external table dim_shift_full(
                                        `id` bigint COMMENT '班次ID',
                                        `line_id` bigint COMMENT '线路ID',
                                        `line_name` string COMMENT '线路名称',
                                        `line_no` string COMMENT '线路编号',
                                        `line_level` string COMMENT '线路级别',
                                        `org_id` bigint COMMENT '所属机构',
                                        `transport_line_type_id` string COMMENT '线路类型ID',
                                        `transport_line_type_name` string COMMENT '线路类型名称',
                                        `start_org_id` bigint COMMENT '起始机构ID',
                                        `start_org_name` string COMMENT '起始机构名称',
                                        `end_org_id` bigint COMMENT '目标机构ID',
                                        `end_org_name` string COMMENT '目标机构名称',
                                        `pair_line_id` bigint COMMENT '配对线路ID',
                                        `distance` decimal(10,2) COMMENT '直线距离',
                                        `cost` decimal(10,2) COMMENT '公路里程',
                                        `estimated_time` bigint COMMENT '预计时间（分钟）',
                                        `start_time` string COMMENT '班次开始时间',
                                        `driver1_emp_id` bigint COMMENT '第一司机',
                                        `driver2_emp_id` bigint COMMENT '第二司机',
                                        `truck_id` bigint COMMENT '卡车ID',
                                        `pair_shift_id` bigint COMMENT '配对班次(同一辆车一去一回的另一班次)'
) comment '班次维度表'
    partitioned by (`dt` string comment '统计周期')
    stored as orc
    location '/warehouse/tms/dim/dim_shift_full'
    tblproperties('orc.compress'='snappy');

-- 6. 司机维度表
-- truck_driver
-- base_organ
-- truck_team
drop table if exists dim_truck_driver_full;
create external table dim_truck_driver_full(
                                               `id` bigint COMMENT '司机信息ID',
                                               `emp_id` bigint COMMENT '员工ID',
                                               `org_id` bigint COMMENT '所属机构ID',
                                               `org_name` string COMMENT '所属机构名称',
                                               `team_id` bigint COMMENT '所属车队ID',
                                               `tream_name` string COMMENT '所属车队名称',
                                               `license_type` string COMMENT '准驾车型',
                                               `init_license_date` string COMMENT '初次领证日期',
                                               `expire_date` string COMMENT '有效截止日期',
                                               `license_no` string COMMENT '驾驶证号',
                                               `is_enabled` tinyint COMMENT '状态 0：禁用 1：正常'
) comment '司机维度表'
    partitioned by (`dt` string comment '统计日期')
    stored as orc
    location '/warehouse/tms/dim/dim_truck_driver_full'
    tblproperties('orc.compress'='snappy');

-- 7. 卡车维度表
-- truck_info
-- truck_model
-- truck_team
-- base_organ
-- base_dic
drop table if exists dim_truck_full;
create external table dim_truck_full(
                                        `id` bigint COMMENT '卡车ID',
                                        `team_id` bigint COMMENT '所属车队ID',
                                        `team_name` string COMMENT '所属车队名称',
                                        `team_no` string COMMENT '车队编号',
                                        `org_id` bigint COMMENT '所属机构',
                                        `org_name` string COMMENT '所属机构名称',
                                        `manager_emp_id` bigint COMMENT '负责人',
                                        `truck_no` string COMMENT '车牌号码',
                                        `truck_model_id` string COMMENT '型号',
                                        `truck_model_name` string COMMENT '型号名称',
                                        `truck_model_type` string COMMENT '型号类型',
                                        `truck_model_type_name` string COMMENT '型号类型名称',
                                        `truck_model_no` string COMMENT '型号编码',
                                        `truck_brand` string COMMENT '品牌',
                                        `truck_brand_name` string COMMENT '品牌名称',
                                        `truck_weight` decimal(16,2) COMMENT '整车重量（吨）',
                                        `load_weight` decimal(16,2) COMMENT '额定载重（吨）',
                                        `total_weight` decimal(16,2) COMMENT '总质量（吨）',
                                        `eev` string COMMENT '排放标准',
                                        `boxcar_len` decimal(16,2) COMMENT '货箱长（m）',
                                        `boxcar_wd` decimal(16,2) COMMENT '货箱宽（m）',
                                        `boxcar_hg` decimal(16,2) COMMENT '货箱高（m）',
                                        `max_speed` bigint COMMENT '最高时速（千米/时）',
                                        `oil_vol` bigint COMMENT '油箱容积（升）',
                                        `device_gps_id` string COMMENT 'GPS设备ID',
                                        `engine_no` string COMMENT '发动机编码',
                                        `license_registration_date` string COMMENT '注册时间',
                                        `license_last_check_date` string COMMENT '最后年检日期',
                                        `license_expire_date` string COMMENT '失效日期',
                                        `is_enabled` tinyint COMMENT '状态 0：禁用 1：正常'
) comment '卡车维度表'
    partitioned by (`dt` string comment '统计日期')
    stored as orc
    location '/warehouse/tms/dim/dim_truck_full'
    tblproperties('orc.compress'='snappy');

-- 8. 用户拉链表
-- user_info
drop table if exists dim_user_zip;
create external table dim_user_zip(
                                      `id` bigint COMMENT '用户地址信息ID',
                                      `login_name` string COMMENT '用户名称',
                                      `nick_name` string COMMENT '用户昵称',
                                      `passwd` string COMMENT '用户密码',
                                      `real_name` string COMMENT '用户姓名',
                                      `phone_num` string COMMENT '手机号',
                                      `email` string COMMENT '邮箱',
                                      `user_level` string COMMENT '用户级别',
                                      `birthday` string COMMENT '用户生日',
                                      `gender` string COMMENT '性别 M男,F女',
                                      `start_date` string COMMENT '起始日期',
                                      `end_date` string COMMENT '结束日期'
) comment '用户拉链表'
    partitioned by (`dt` string comment '统计日期')
    stored as orc
    location '/warehouse/tms/dim/dim_user_zip'
    tblproperties('orc.compress'='snappy');

-- 9. 用户地址拉链表
-- user_address
drop table if exists dim_user_address_zip;
create external table dim_user_address_zip(
                                              `id` bigint COMMENT '地址ID',
                                              `user_id` bigint COMMENT '用户ID',
                                              `phone` string COMMENT '电话号',
                                              `province_id` bigint COMMENT '所属省份ID',
                                              `city_id` bigint COMMENT '所属城市ID',
                                              `district_id` bigint COMMENT '所属区县ID',
                                              `complex_id` bigint COMMENT '所属小区ID',
                                              `address` string COMMENT '详细地址',
                                              `is_default` tinyint COMMENT '是否默认',
                                              `start_date` string COMMENT '起始日期',
                                              `end_date` string COMMENT '结束日期'
) comment '用户地址拉链表'
    partitioned by (`dt` string comment '统计日期')
    stored as orc
    location '/warehouse/tms/dim/dim_user_address_zip'
    tblproperties('orc.compress'='snappy');