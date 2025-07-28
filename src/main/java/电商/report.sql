
use gmall;

-- TODO：11.3 商品主题 报表 分析


---------------------------------------------------TODO: 11.3 商品主题---------------------------------------------------

--TODO: 3.1. ads_repeat_purchase_by_tm
-- 11.3.1 最近30日各品牌复购率
DROP TABLE IF EXISTS ads_repeat_purchase_by_tm;
CREATE EXTERNAL TABLE ads_repeat_purchase_by_tm
(
    `dt`                  STRING COMMENT '统计日期',
    `recent_days`       BIGINT COMMENT '最近天数,30:最近30天',
    `tm_id`              STRING COMMENT '品牌ID',
    `tm_name`            STRING COMMENT '品牌名称',
    `order_repeat_rate` DECIMAL(16, 2) COMMENT '复购率'
) COMMENT '最近30日各品牌复购率统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_repeat_purchase_by_tm';

insert overwrite table ads_repeat_purchase_by_tm
select * from ads_repeat_purchase_by_tm
union
select
    '2025-07-28',
    30,
    tm_id,
    tm_name,
    cast(sum(if(order_count>=2,1,0))/sum(if(order_count>=1,1,0)) as decimal(16,2))
from
    (
        select
            user_id,
            tm_id,
            tm_name,
            sum(order_count_30d) order_count
        from dws_trade_user_sku_order_nd
        where dt='2025-07-28'
        group by user_id, tm_id,tm_name
    )t1
group by tm_id,tm_name;
select * from ads_repeat_purchase_by_tm;
-- 1.最近30日各品牌复购率


--TODO: 3.2. ads_order_stats_by_tm
-- 11.3.2 各品牌商品下单统计
DROP TABLE IF EXISTS ads_order_stats_by_tm;
CREATE EXTERNAL TABLE ads_order_stats_by_tm
(
    `dt`                      STRING COMMENT '统计日期',
    `recent_days`             BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `tm_id`                   STRING COMMENT '品牌ID',
    `tm_name`                 STRING COMMENT '品牌名称',
    `order_count`             BIGINT COMMENT '下单数',
    `order_user_count`        BIGINT COMMENT '下单人数'
) COMMENT '各品牌商品下单统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_order_stats_by_tm';

insert overwrite table ads_order_stats_by_tm
select * from ads_order_stats_by_tm
union
select
    '2025-07-28' dt,
    recent_days,
    tm_id,
    tm_name,
    order_count,
    order_user_count
from
    (
        select
            1 recent_days,
            tm_id,
            tm_name,
            sum(order_count_1d) order_count,
            count(distinct(user_id)) order_user_count
        from dws_trade_user_sku_order_1d
        where dt='2025-07-28'
        group by tm_id,tm_name
        union all
        select
            recent_days,
            tm_id,
            tm_name,
            sum(order_count),
            count(distinct(if(order_count>0,user_id,null)))
        from
            (
                select
                    recent_days,
                    user_id,
                    tm_id,
                    tm_name,
                    case recent_days
                        when 7 then order_count_7d
                        when 30 then order_count_30d
                        end order_count
                from dws_trade_user_sku_order_nd lateral view explode(array(7,30)) tmp as recent_days
                where dt='2025-07-28'
            )t1
        group by recent_days,tm_id,tm_name
    )odr;
select * from ads_order_stats_by_tm;
-- 2.各品牌商品下单数


--TODO: 3.3. ads_order_stats_by_cate
-- 11.3.3 各品类商品下单统计
DROP TABLE IF EXISTS ads_order_stats_by_cate;
CREATE EXTERNAL TABLE ads_order_stats_by_cate
(
    `dt`                      STRING COMMENT '统计日期',
    `recent_days`             BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `category1_id`            STRING COMMENT '一级品类ID',
    `category1_name`          STRING COMMENT '一级品类名称',
    `category2_id`            STRING COMMENT '二级品类ID',
    `category2_name`          STRING COMMENT '二级品类名称',
    `category3_id`            STRING COMMENT '三级品类ID',
    `category3_name`          STRING COMMENT '三级品类名称',
    `order_count`             BIGINT COMMENT '下单数',
    `order_user_count`        BIGINT COMMENT '下单人数'
) COMMENT '各品类商品下单统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_order_stats_by_cate';

insert overwrite table ads_order_stats_by_cate
select * from ads_order_stats_by_cate
union
select
    '2025-07-28' dt,
    recent_days,
    category1_id,
    category1_name,
    category2_id,
    category2_name,
    category3_id,
    category3_name,
    order_count,
    order_user_count
from
    (
        select
            1 recent_days,
            category1_id,
            category1_name,
            category2_id,
            category2_name,
            category3_id,
            category3_name,
            sum(order_count_1d) order_count,
            count(distinct(user_id)) order_user_count
        from dws_trade_user_sku_order_1d
        where dt='2025-07-28'
        group by category1_id,category1_name,category2_id,category2_name,category3_id,category3_name
        union all
        select
            recent_days,
            category1_id,
            category1_name,
            category2_id,
            category2_name,
            category3_id,
            category3_name,
            sum(order_count),
            count(distinct(if(order_count>0,user_id,null)))
        from
            (
                select
                    recent_days,
                    user_id,
                    category1_id,
                    category1_name,
                    category2_id,
                    category2_name,
                    category3_id,
                    category3_name,
                    case recent_days
                        when 7 then order_count_7d
                        when 30 then order_count_30d
                        end order_count
                from dws_trade_user_sku_order_nd lateral view explode(array(7,30)) tmp as recent_days
                where dt='2025-07-28'
            )t1
        group by recent_days,category1_id,category1_name,category2_id,category2_name,category3_id,category3_name
    )odr;
select * from ads_order_stats_by_cate;
-- 3.每个二级品类名称的下单人数


--TODO: 3.4. ads_sku_cart_num_top3_by_cate
-- 11.3.4 各品类商品购物车存量Top3
DROP TABLE IF EXISTS ads_sku_cart_num_top3_by_cate;
CREATE EXTERNAL TABLE ads_sku_cart_num_top3_by_cate
(
    `dt`             STRING COMMENT '统计日期',
    `category1_id`   STRING COMMENT '一级品类ID',
    `category1_name` STRING COMMENT '一级品类名称',
    `category2_id`   STRING COMMENT '二级品类ID',
    `category2_name` STRING COMMENT '二级品类名称',
    `category3_id`   STRING COMMENT '三级品类ID',
    `category3_name` STRING COMMENT '三级品类名称',
    `sku_id`         STRING COMMENT 'SKU_ID',
    `sku_name`       STRING COMMENT 'SKU名称',
    `cart_num`       BIGINT COMMENT '购物车中商品数量',
    `rk`             BIGINT COMMENT '排名'
) COMMENT '各品类商品购物车存量Top3'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_sku_cart_num_top3_by_cate';

-- 当数据中的Hash表结构为空时抛出类型转换异常，禁用相应优化即可
set hive.mapjoin.optimized.hashtable=false;
insert overwrite table ads_sku_cart_num_top3_by_cate
select * from ads_sku_cart_num_top3_by_cate
union
select
    '20250728' dt,
    category1_id,
    category1_name,
    category2_id,
    category2_name,
    category3_id,
    category3_name,
    sku_id,
    sku_name,
    cart_num,
    rk
from
    (
        select
            sku_id,
            sku_name,
            category1_id,
            category1_name,
            category2_id,
            category2_name,
            category3_id,
            category3_name,
            cart_num,
            rank() over (partition by category1_id,category2_id,category3_id order by cart_num desc) rk
        from
            (
                select
                    sku_id,
                    sum(sku_num) cart_num
                from dwd_trade_cart_full
                where dt='20250728'
                group by sku_id
            )cart
                left join
            (
                select
                    id,
                    sku_name,
                    category1_id,
                    category1_name,
                    category2_id,
                    category2_name,
                    category3_id,
                    category3_name
                from dim_sku_full
                where dt='20211214'
            )sku
            on cart.sku_id=sku.id
    )t1
where rk<=3;
-- 优化项不应一直禁用，受影响的SQL执行完毕后打开
set hive.mapjoin.optimized.hashtable=true;
select * from ads_sku_cart_num_top3_by_cate;
-- 4.各个商品的商品数量



--TODO: 3.5. ads_sku_favor_count_top3_by_tm
-- 11.3.5 各品牌商品收藏次数Top3
DROP TABLE IF EXISTS ads_sku_favor_count_top3_by_tm;
CREATE EXTERNAL TABLE ads_sku_favor_count_top3_by_tm
(
    `dt`          STRING COMMENT '统计日期',
    `tm_id`       STRING COMMENT '品牌ID',
    `tm_name`     STRING COMMENT '品牌名称',
    `sku_id`      STRING COMMENT 'SKU_ID',
    `sku_name`    STRING COMMENT 'SKU名称',
    `favor_count` BIGINT COMMENT '被收藏次数',
    `rk`          BIGINT COMMENT '排名'
) COMMENT '各品牌商品收藏次数Top3'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/ads/ads_sku_favor_count_top3_by_tm';

insert overwrite table ads_sku_favor_count_top3_by_tm
select * from ads_sku_favor_count_top3_by_tm
union
select
    '2025-07-28' dt,
    tm_id,
    tm_name,
    sku_id,
    sku_name,
    favor_add_count_1d,
    rk
from
    (
        select
            tm_id,
            tm_name,
            sku_id,
            sku_name,
            favor_add_count_1d,
            rank() over (partition by tm_id order by favor_add_count_1d desc) rk
        from dws_interaction_sku_favor_add_1d
        where dt='2025-07-28'
    )t1
where rk<=3;
select * from ads_sku_favor_count_top3_by_tm;
-- 5.品牌名称被收藏的次数



--TODO： 1.最近30日各品牌复购率
--TODO： 2.各品牌商品下单数
--TODO： 3.每个二级品类名称的下单人数
--TODO： 4.各个商品的商品数量
--TODO： 5.品牌名称被收藏的次数