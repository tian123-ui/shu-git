
=========================================================================================================

2. 每日趋势数据（用于折线图展示）

-- 按日期分组统计发送次数和核销率
SELECT 
    DATE(s.send_time) AS 统计日期,
    COUNT(DISTINCT s.send_id) AS 当日发送次数,
    COUNT(DISTINCT u.use_id) AS 当日核销次数,
    ROUND(
        (COUNT(DISTINCT CASE WHEN u.is_valid_use = 1 THEN u.use_id END) 
        / COUNT(DISTINCT s.send_id)) * 100, 2
    ) AS 当日核销率百分比
FROM 
    dwd_customer_service_promo_send s
LEFT JOIN 
    dwd_customer_service_promo_use u 
    ON s.send_id = u.send_id
WHERE 
    s.send_time BETWEEN '2025-08-01 00:00:00' AND '2025-08-31 23:59:59'
GROUP BY 
    DATE(s.send_time)
ORDER BY 
    统计日期;



=========================================================================================================

select * from dwd_customer_service_promo_activity;
select * from dwd_customer_service_promo_send;
select * from dwd_customer_service_promo_use;


二、活动效果分析报表 SQL
1. 活动列表及核心指标（区分进行中 / 已结束）

SELECT 
    a.activity_id AS 活动ID,
    a.activity_name AS 活动名称,
    a.activity_level AS 活动级别,
    a.promo_type AS 优惠类型,
    a.`status` AS 活动状态,  -- 直接使用表中已有的 `status` 字段
    COUNT(DISTINCT s.send_id) AS 累计发送次数,  -- 统计活动的发送次数
    COUNT(DISTINCT u.use_id) AS 累计核销次数,  -- 统计活动的核销次数
    -- 计算核销率：(核销次数 / 发送次数) * 100，避免除以 0
    ROUND(
        (COUNT(DISTINCT u.use_id) / NULLIF(COUNT(DISTINCT s.send_id), 0)) * 100, 
        2
    ) AS 活动核销率百分比
FROM 
    dwd_customer_service_promo_activity a  -- 活动主表
LEFT JOIN 
    dwd_customer_service_promo_send s  -- 发送记录表
    ON a.activity_id = s.activity_id  -- 通过 activity_id 关联
LEFT JOIN 
    dwd_customer_service_promo_use u  -- 核销记录表
    ON s.send_id = u.send_id  -- 通过 send_id 关联
GROUP BY 
    a.activity_id, a.activity_name, a.activity_level, a.promo_type, a.`status`  -- 按活动维度分组
ORDER BY 
    a.activity_id;  -- 按活动 ID 排序


-- =========================================================================================================


三、客服绩效评估报表 SQL
1. 客服个人业绩排行

select * from dim_customer_service;
select * from dwd_customer_service_promo_send;
select * from dwd_customer_service_promo_use;
select * from dim_shop;


-- 按客服分组统计，支持按核销率/支付金额排序
 
-- 客服个人业绩排行统计
-- 
SELECT 
    cs.customer_service_id AS 客服ID,
    cs.cs_name AS 客服姓名,
    shop.shop_name AS 所属店铺,
    COUNT(DISTINCT send.send_id) AS 发送次数,
    COUNT(DISTINCT send.send_id) AS 发送总金额, -- 替换为实际金额字段（如SUM）
    COUNT(DISTINCT `use`.use_id) AS 核销订单数,
    COUNT(DISTINCT `use`.use_id) AS 核销支付总金额, -- 替换为实际金额字段（如SUM）
    ROUND(
        (COUNT(DISTINCT CASE WHEN `use`.is_valid_use = 1 THEN `use`.use_id END) 
         / NULLIF(COUNT(DISTINCT send.send_id), 0)) * 100, 2
    ) AS 核销率百分比
FROM 
    dim_customer_service cs
-- 关联发送记录表
LEFT JOIN 
    dwd_customer_service_promo_send send 
    ON cs.customer_service_id = send.customer_service_id
-- 关联核销记录表（用反引号转义关键字use）
LEFT JOIN 
    dwd_customer_service_promo_use `use` 
    ON send.send_id = `use`.send_id
-- 关联店铺信息表（修正表名拼写错误）
LEFT JOIN 
    dim_shop shop 
    ON cs.shop_id = shop.shop_id 
GROUP BY 
    cs.customer_service_id, 
    cs.cs_name, 
    shop.shop_name 
ORDER BY 
    核销率百分比 DESC, 
    核销支付总金额 DESC;





 


=========================================================================================================



四、用户分层效果分析 SQL
1. 不同用户层级的优惠转化效果


select * FROM dwd_customer_service_promo_send;

SELECT * FROM dwd_customer_service_promo_use;


-- 关联RFM用户分层，分析各层级用户的核销表现
-- 不同用户层级的优惠转化效果分析
SELECT 
    CASE 
        WHEN SUM(CASE WHEN `use`.is_valid_use = 1 THEN 1 ELSE 0 END) > 5 THEN '高价值用户'  
        WHEN SUM(CASE WHEN `use`.is_valid_use = 1 THEN 1 ELSE 0 END) BETWEEN 3 AND 5 THEN '潜力用户'
        WHEN SUM(CASE WHEN `use`.is_valid_use = 1 THEN 1 ELSE 0 END) BETWEEN 1 AND 2 THEN '一般用户'
        ELSE '流失用户'
    END AS 用户层级,
    COUNT(DISTINCT s.send_id) AS 发送次数,
    COUNT(DISTINCT `use`.use_id) AS 核销总次数,
    COUNT(DISTINCT CASE WHEN `use`.is_valid_use = 1 THEN `use`.use_id END) AS 有效核销次数,
    ROUND(
        (COUNT(DISTINCT CASE WHEN `use`.is_valid_use = 1 THEN `use`.use_id END) 
         / NULLIF(COUNT(DISTINCT s.send_id), 0)) * 100, 2
    ) AS 核销率百分比,
    COUNT(`use`.use_id) AS 支付总次数
FROM 
    dwd_customer_service_promo_send s
LEFT JOIN 
    dwd_customer_service_promo_use `use` 
    ON s.send_id = `use`.send_id
GROUP BY 
    s.customer_service_id, s.product_id, s.activity_id
-- 直接使用 CASE 表达式排序，避免引用别名
ORDER BY 
    FIELD(
        CASE 
            WHEN SUM(CASE WHEN `use`.is_valid_use = 1 THEN 1 ELSE 0 END) > 5 THEN '高价值用户'  
            WHEN SUM(CASE WHEN `use`.is_valid_use = 1 THEN 1 ELSE 0 END) BETWEEN 3 AND 5 THEN '潜力用户'
            WHEN SUM(CASE WHEN `use`.is_valid_use = 1 THEN 1 ELSE 0 END) BETWEEN 1 AND 2 THEN '一般用户'
            ELSE '流失用户'
        END, 
        '高价值用户', '潜力用户', '一般用户', '流失用户'
    );













