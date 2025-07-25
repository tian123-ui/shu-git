use tms;

------------------------------TODO:10.1 最近 1 日汇总表------------------------------------

--TODO: 1.1 dws_trade_org_cargo_type_order_1d
-- 1.1 交易域机构货物类型粒度下单 1 日汇总表

-- （1）首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dws_trade_org_cargo_type_order_1d
    partition (dt)
select org_id,
       org_name,
       city_id,
       region.name city_name,
       cargo_type,
       cargo_type_name,
       order_count,
       order_amount,
       dt
from (select org_id,
             org_name,
             sender_city_id  city_id,
             cargo_type,
             cargo_type_name,
             count(order_id) order_count,
             sum(amount)     order_amount,
             dt
      from (select order_id,
                   cargo_type,
                   cargo_type_name,
                   sender_district_id,
                   sender_city_id,
                   max(amount) amount,
                   dt
            from (select order_id,
                         cargo_type,
                         cargo_type_name,
                         sender_district_id,
                         sender_city_id,
                         amount,
                         dt
                  from dwd_trade_order_detail) detail
            group by order_id,
                     cargo_type,
                     cargo_type_name,
                     sender_district_id,
                     sender_city_id,
                     dt) distinct_detail
               left join
           (select id org_id,
                   org_name,
                   region_id
            from dim_organ
            where dt = '20250723') org
           on distinct_detail.sender_district_id = org.region_id
      group by org_id,
               org_name,
               cargo_type,
               cargo_type_name,
               sender_city_id,
               dt) agg
         left join (
    select id,
           name
    from dim_region
    where dt = '20200623'
) region on city_id = region.id;
select * from dws_trade_org_cargo_type_order_1d;

-- （2）每日装载
insert overwrite table dws_trade_org_cargo_type_order_1d
    partition (dt = '20250722')
select org_id,
       org_name,
       city_id,
       region.name city_name,
       cargo_type,
       cargo_type_name,
       order_count,
       order_amount
from (select org_id,
             org_name,
             city_id,
             cargo_type,
             cargo_type_name,
             count(order_id) order_count,
             sum(amount)     order_amount
      from (select order_id,
                   cargo_type,
                   cargo_type_name,
                   sender_district_id,
                   sender_city_id city_id,
                   sum(amount)    amount
            from (select order_id,
                         cargo_type,
                         cargo_type_name,
                         sender_district_id,
                         sender_city_id,
                         amount
                  from dwd_trade_order_detail
                  where dt = '2025-07-22') detail
            group by order_id,
                     cargo_type,
                     cargo_type_name,
                     sender_district_id,
                     sender_city_id) distinct_detail
               left join
           (select id org_id,
                   org_name,
                   region_id
            from dim_organ
            where dt = '20250723') org
           on distinct_detail.sender_district_id = org.region_id
      group by org_id,
               org_name,
               city_id,
               cargo_type,
               cargo_type_name) agg
         left join (
    select id,
           name
    from dim_region
    where dt = '20250723'
) region on city_id = region.id;
select * from dws_trade_org_cargo_type_order_1d;



--TODO:1.2 dws_trans_org_receive_1d
-- 10.1.2 物流域转运站粒度揽收 1 日汇总表

-- （1）首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dws_trans_org_receive_1d
    partition (dt)
select org_id,
       org_name,
       city_id,
       city_name,
       province_id,
       province_name,
       count(order_id)      order_count,
       sum(distinct_amount) order_amount,
       dt
from (select order_id,
             org_id,
             org_name,
             city_id,
             city_name,
             province_id,
             province_name,
             max(amount) distinct_amount,
             dt
      from (select order_id,
                   amount,
                   sender_district_id,
                   dt
            from dwd_trans_receive_detail) detail
               left join
           (select id org_id,
                   org_name,
                   region_id
            from dim_organ
            where dt = '20250723') organ
           on detail.sender_district_id = organ.region_id
               left join
           (select id,
                   parent_id
            from dim_region
            where dt = '20200623') district
           on region_id = district.id
               left join
           (select id   city_id,
                   name city_name,
                   parent_id
            from dim_region
            where dt = '20200623') city
           on district.parent_id = city_id
               left join
           (select id   province_id,
                   name province_name,
                   parent_id
            from dim_region
            where dt = '20200623') province
           on city.parent_id = province_id
      group by order_id,
               org_id,
               org_name,
               city_id,
               city_name,
               province_id,
               province_name,
               dt) distinct_tb
group by org_id,
         org_name,
         city_id,
         city_name,
         province_id,
         province_name,
         dt;
select * from dws_trans_org_receive_1d;

-- （2）每日装载
insert overwrite table dws_trans_org_receive_1d
    partition (dt = '20250722')
select org_id,
       org_name,
       city_id,
       city_name,
       province_id,
       province_name,
       count(order_id)      order_count,
       sum(distinct_amount) order_amount
from (select order_id,
             org_id,
             org_name,
             city_id,
             city_name,
             province_id,
             province_name,
             max(amount) distinct_amount
      from (select order_id,
                   amount,
                   sender_district_id
            from dwd_trans_receive_detail
            where dt = '2025-07-22') detail
               left join
           (select id org_id,
                   org_name,
                   region_id
            from dim_organ
            where dt = '20200623') organ
           on detail.sender_district_id = organ.region_id
               left join
           (select id,
                   parent_id
            from dim_region
            where dt = '20200623') district
           on region_id = district.id
               left join
           (select id   city_id,
                   name city_name,
                   parent_id
            from dim_region
            where dt = '20200623') city
           on district.parent_id = city_id
               left join
           (select id   province_id,
                   name province_name,
                   parent_id
            from dim_region
            where dt = '20200623') province
           on city.parent_id = province_id
      group by order_id,
               org_id,
               org_name,
               city_id,
               city_name,
               province_id,
               province_name) distinct_tb
group by org_id,
         org_name,
         city_id,
         city_name,
         province_id,
         province_name;
select * from dws_trans_org_receive_1d;



--TODO:1.3 dws_trans_dispatch_1d
-- 10.1.3 物流域发单 1 日汇总表

-- （1）首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dws_trans_dispatch_1d
    partition (dt)
select count(order_id)      order_count,
       sum(distinct_amount) order_amount,
       dt
from (select order_id,
             dt,
             max(amount) distinct_amount
      from dwd_trans_dispatch_detail
      group by order_id,
               dt) distinct_info
group by dt;
select * from dws_trans_dispatch_1d;

-- （2）每日装载
insert overwrite table dws_trans_dispatch_1d
    partition (dt = '20250722')
select count(order_id)      order_count,
       sum(distinct_amount) order_amount
from (select order_id,
             max(amount) distinct_amount
      from dwd_trans_dispatch_detail
      where dt = '2025-07-22'
      group by order_id) distinct_info;
select * from dws_trans_dispatch_1d;



--TODO: 1.4dws_trans_org_truck_model_type_trans_finish_1d
-- 10.1.4 物流域机构卡车类别粒度运输最近 1 日汇总表

-- （1）首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dws_trans_org_truck_model_type_trans_finish_1d
    partition (dt)
select org_id,
       org_name,
       truck_model_type,
       truck_model_type_name,
       count(trans_finish.id) truck_finish_count,
       sum(actual_distance)   trans_finish_distance,
       sum(finish_dur_sec)    finish_dur_sec,
       dt
from (select id,
             start_org_id   org_id,
             start_org_name org_name,
             truck_id,
             actual_distance,
             finish_dur_sec,
             dt
      from dwd_trans_trans_finish) trans_finish
         left join
     (select id,
             truck_model_type,
             truck_model_type_name
      from dim_truck
      where dt = '20250723') truck_info
     on trans_finish.truck_id = truck_info.id
group by org_id,
         org_name,
         truck_model_type,
         truck_model_type_name,
         dt;
select * from dws_trans_org_truck_model_type_trans_finish_1d;

-- （2）每日装载
insert overwrite table dws_trans_org_truck_model_type_trans_finish_1d
    partition (dt = '20250722')
select org_id,
       org_name,
       truck_model_type,
       truck_model_type_name,
       count(trans_finish.id) truck_finish_count,
       sum(actual_distance)   trans_finish_distance,
       sum(finish_dur_sec)    finish_dur_sec
from (select id,
             start_org_id   org_id,
             start_org_name org_name,
             truck_id,
             actual_distance,
             finish_dur_sec
      from dwd_trans_trans_finish
      where dt = '20250722') trans_finish
         left join
     (select id,
             truck_model_type,
             truck_model_type_name
      from dim_truck
      where dt = '20250723') truck_info
     on trans_finish.truck_id = truck_info.id
group by org_id,
         org_name,
         truck_model_type,
         truck_model_type_name;
select * from dws_trans_org_truck_model_type_trans_finish_1d;



--TODO: 1.5 dws_trans_org_deliver_suc_1d
-- 10.1.5 物流域转运站粒度派送成功 1 日汇总表

-- （1）首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dws_trans_org_deliver_suc_1d
    partition (dt)
select org_id,
       org_name,
       city_id,
       city.name       city_name,
       province_id,
       province.name   province_name,
       count(order_id) order_count,
       dt
from (select order_id,
             receiver_district_id,
             dt
      from dwd_trans_deliver_suc_detail
      group by order_id, receiver_district_id, dt) detail
         left join
     (select id        org_id,
             org_name,
             region_id district_id
      from dim_organ
      where dt = '20250723') organ
     on detail.receiver_district_id = organ.district_id
         left join
     (select id,
             parent_id city_id
      from dim_region
      where dt = '20200623') district
     on district_id = district.id
         left join
     (select id,
             name,
             parent_id province_id
      from dim_region
      where dt = '20200623') city
     on city_id = city.id
         left join
     (select id,
             name
      from dim_region
      where dt = '20200623') province
     on province_id = province.id
group by org_id,
         org_name,
         city_id,
         city.name,
         province_id,
         province.name,
         dt;
select * from dws_trans_org_deliver_suc_1d;


--TODO: 1.6 dws_trans_org_sort_1d
-- 10.1.6 物流域机构粒度分拣 1 日汇总表

-- （1）首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dws_trans_org_sort_1d
    partition (dt)
select org_id,
       org_name,
       if(org_level = 1, city_for_level1.id, province_for_level1.id)         city_id,
       if(org_level = 1, city_for_level1.name, province_for_level1.name)     city_name,
       if(org_level = 1, province_for_level1.id, province_for_level2.id)     province_id,
       if(org_level = 1, province_for_level1.name, province_for_level2.name) province_name,
       sort_count,
       dt
from (select org_id,
             count(*) sort_count,
             dt
      from dwd_bound_sort
      group by org_id, dt) agg
         left join
     (select id,
             org_name,
             org_level,
             region_id
      from dim_organ
      where dt = '20250723') org
     on org_id = org.id
         left join
     (select id,
             name,
             parent_id
      from dim_region
      where dt = '20200623') city_for_level1
     on region_id = city_for_level1.id
         left join
     (select id,
             name,
             parent_id
      from dim_region
      where dt = '20200623') province_for_level1
     on city_for_level1.parent_id = province_for_level1.id
         left join
     (select id,
             name,
             parent_id
      from dim_region
      where dt = '20200623') province_for_level2
     on province_for_level1.parent_id = province_for_level2.id;
select * from dws_trans_org_sort_1d;

-- （2）每日装载
insert overwrite table dws_trans_org_sort_1d
    partition (dt = '2025-07-22')
select org_id,
       org_name,
       if(org_level = 1, city_for_level1.id, province_for_level1.id)         city_id,
       if(org_level = 1, city_for_level1.name, province_for_level1.name)     city_name,
       if(org_level = 1, province_for_level1.id, province_for_level2.id)     province_id,
       if(org_level = 1, province_for_level1.name, province_for_level2.name) province_name,
       sort_count
from (select org_id,
             count(*) sort_count
      from dwd_bound_sort
      where dt = '20250722'
      group by org_id) agg
         left join
     (select id,
             org_name,
             org_level,
             region_id
      from dim_organ
      where dt = '20200723') org
     on org_id = org.id
         left join
     (select id,
             name,
             parent_id
      from dim_region
      where dt = '20200623') city_for_level1
     on region_id = city_for_level1.id
         left join
     (select id,
             name,
             parent_id
      from dim_region
      where dt = '20200623') province_for_level1
     on city_for_level1.parent_id = province_for_level1.id
         left join
     (select id,
             name,
             parent_id
      from dim_region
      where dt = '20200623') province_for_level2
     on province_for_level1.parent_id = province_for_level2.id;
select * from dws_trans_org_sort_1d;


------------------------------TODO:10.2 最近 n 日汇总表------------------------------------

--TODO: 2.1 dws_trade_org_cargo_type_order_nd
-- 10.2.1 交易域机构货物类型粒度下单 n 日汇总表
insert overwrite table dws_trade_org_cargo_type_order_nd
    partition (dt = '2025-07-22')
select org_id,
       org_name,
       city_id,
       city_name,
       cargo_type,
       cargo_type_name,
       recent_days,
       sum(order_count)  order_count,
       sum(order_amount) order_amount
from dws_trade_org_cargo_type_order_1d lateral view
         explode(array(7, 30)) tmp as recent_days
where dt >= date_add('2025-07-22', -recent_days + 1)
group by org_id,
         org_name,
         city_id,
         city_name,
         cargo_type,
         cargo_type_name,
         recent_days;
select * from dws_trade_org_cargo_type_order_nd;


--TODO： 2.2 dws_trans_org_receive_nd
-- 10.2.2 物流域转运站粒度揽收 n 日汇总表
insert overwrite table dws_trans_org_receive_nd
    partition (dt = '2025-07-22')
select org_id,
       org_name,
       city_id,
       city_name,
       province_id,
       province_name,
       recent_days,
       sum(order_count)  order_count,
       sum(order_amount) order_amount
from dws_trans_org_receive_1d
         lateral view explode(array(7, 30)) tmp as recent_days
where dt >= date_add('2025-07-22', -recent_days + 1)
group by org_id,
         org_name,
         city_id,
         city_name,
         province_id,
         province_name,
         recent_days;
select * from dws_trans_org_receive_nd;


--TODO: 2.3 dws_trans_dispatch_nd
-- 10.2.3 物流域发单 n 日汇总表
insert overwrite table dws_trans_dispatch_nd
    partition (dt = '2025-07-23')
select recent_days,
       sum(order_count)  order_count,
       sum(order_amount) order_amount
from dws_trans_dispatch_1d lateral view
         explode(array(7, 30)) tmp as recent_days
where dt >= date_add('2025-07-23', -recent_days + 1)
group by recent_days;
select * from dws_trans_dispatch_nd;



--TODO: 2.4 dws_trans_shift_trans_finish_nd
-- 10.2.4 物流域班次粒度转运完成最近 n 日汇总表
insert overwrite table dws_trans_shift_trans_finish_nd
    partition (dt = '2025-07-22')
select shift_id,
       if(org_level = 1, first.region_id, city.id)     city_id,
       if(org_level = 1, first.region_name, city.name) city_name,
       org_id,
       org_name,
       line_id,
       line_name,
       driver1_emp_id,
       driver1_name,
       driver2_emp_id,
       driver2_name,
       truck_model_type,
       truck_model_type_name,
       recent_days,
       trans_finish_count,
       trans_finish_distance,
       trans_finish_dur_sec,
       trans_finish_order_count,
       trans_finish_delay_count
from (select recent_days,
             shift_id,
             line_id,
             truck_id,
             start_org_id                                       org_id,
             start_org_name                                     org_name,
             driver1_emp_id,
             driver1_name,
             driver2_emp_id,
             driver2_name,
             count(id)                                          trans_finish_count,
             sum(actual_distance)                               trans_finish_distance,
             sum(finish_dur_sec)                                trans_finish_dur_sec,
             sum(order_num)                                     trans_finish_order_count,
             sum(if(actual_end_time > estimate_end_time, 1, 0)) trans_finish_delay_count
      from dwd_trans_trans_finish lateral view
               explode(array(7, 30)) tmp as recent_days
      where dt >= date_add('2025-07-22', -recent_days + 1)
      group by recent_days,
               shift_id,
               line_id,
               start_org_id,
               start_org_name,
               driver1_emp_id,
               driver1_name,
               driver2_emp_id,
               driver2_name,
               truck_id) aggregated
         left join
     (select id,
             org_level,
             region_id,
             region_name
      from dim_organ
      where dt = '20250723'
     ) first
     on aggregated.org_id = first.id
         left join
     (select id,
             parent_id
      from dim_region
      where dt = '20200623'
     ) parent
     on first.region_id = parent.id
         left join
     (select id,
             name
      from dim_region
      where dt = '20200623'
     ) city
     on parent.parent_id = city.id
         left join
     (select id,
             line_name
      from dim_shift
      where dt = '20250723') for_line_name
     on shift_id = for_line_name.id
         left join (
    select id,
           truck_model_type,
           truck_model_type_name
    from dim_truck
    where dt = '20250723'
) truck_info on truck_id = truck_info.id;
select * from dws_trans_shift_trans_finish_nd;



--TODO: 2.5 dws_trans_org_deliver_suc_nd
-- 10.2.5 物流域转运站粒度派送成功 n 日汇总表
insert overwrite table dws_trans_org_deliver_suc_nd
    partition (dt = '2025-07-23')
select org_id,
       org_name,
       city_id,
       city_name,
       province_id,
       province_name,
       recent_days,
       sum(order_count) as  order_count
from dws_trans_org_deliver_suc_1d lateral view
         explode(array(7, 30)) tmp as recent_days
where dt >= date_add('2025-07-23', -recent_days + 1)
group by org_id,
         org_name,
         city_id,
         city_name,
         province_id,
         province_name,
         recent_days;
select * from dws_trans_org_deliver_suc_nd;



--TODO: 2.6 dws_trans_org_sort_nd
-- 10.2.6 物流域机构粒度分拣 n 日汇总表
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dws_trans_org_sort_nd
    partition (dt = '2025-07-22')
select org_id,
       org_name,
       city_id,
       city_name,
       province_id,
       province_name,
       recent_days,
       sum(sort_count) as  sort_count
from dws_trans_org_sort_1d lateral view
         explode(array(7, 30)) tmp as recent_days
where dt >= date_add('2025-07-22', -recent_days + 1)
group by org_id,
         org_name,
         city_id,
         city_name,
         province_id,
         province_name,
         recent_days;
select * from dws_trans_org_sort_nd;



------------------------------TODO: 10.3 历史至今汇总表------------------------------------

--TODO: 3.1 dws_trans_dispatch_td
-- 10.3.1 物流域发单历史至今汇总表

-- （1）首日装载
insert overwrite table dws_trans_dispatch_td
    partition (dt = '2025-07-23')
select sum(order_count) as  order_count,
       sum(order_amount) as order_amount
from dws_trans_dispatch_1d;
select * from dws_trans_dispatch_td;

-- （2）每日装载
insert overwrite table dws_trans_dispatch_td
    partition (dt = '2025-07-23')
select sum(order_count) as order_count,
       sum(order_amount) as order_amount
from (select order_count,
             order_amount
      from dws_trans_dispatch_td
      where dt = date_add('2025-07-23', -1)
      union
      select order_count,
             order_amount
      from dws_trans_dispatch_1d
      where dt = '2025-07-23') all_data;
select * from dws_trans_dispatch_td;



--TODO: 3.2 dws_trans_bound_finish_td
-- 10.3.2 物流域转运完成历史至今汇总表

-- （1）首日装载
insert overwrite table dws_trans_bound_finish_td
    partition (dt = '2025-07-23')
select count(order_id)   order_count,
       sum(order_amount) order_amount
from (select order_id,
             max(amount) order_amount
      from dwd_trans_bound_finish_detail
      group by order_id) distinct_info;
select * from dws_trans_bound_finish_td;

-- （2）每日装载
insert overwrite table dws_trans_bound_finish_td
    partition (dt = '2025-07-23')
select sum(order_count)  order_count,
       sum(order_amount) order_amount
from (select order_count,
             order_amount
      from dws_trans_bound_finish_td
      where dt = date_add('2025-07-23', -1)
      union
      select count(order_id)   order_count,
             sum(order_amount) order_amount
      from (select order_id,
                   max(amount) order_amount
            from dwd_trans_bound_finish_detail
            where dt = '2025-07-23'
            group by order_id) distinct_tb) all_data;
select * from dws_trans_bound_finish_td;


