




-- 新老客户占比dim_product_sku分析图， 

select * from dim_user;

SELECT * FROM dwd_rfm_behavior_detail;





SELECT * FROM dwd_rfm_behavior_detail;



SELECT * FROM dim_product_sku;


select u.`用户标签`,
       COUNT(DISTINCT `用户ID`) /   
from dim_user u
LEFT JOIN dwd_rfm_behavior_detail r on u.`用户ID`=r.`用户ID`
GROUP BY `用户标签`


LEFT JOIN dwd_rfm_behavior_detail ;


 -- 计算占比（保留2位小数，转百分比）
    CONCAT(
        ROUND(
            COUNT(DISTINCT ubo.user_id) 
            / NULLIF(  -- 避免分母为0
                COUNT(DISTINCT ubo.user_id) OVER (PARTITION BY CASE 
                    WHEN ubo.behavior_type = 'search' THEN 'search'
                    WHEN ubo.behavior_type = 'visit'  THEN 'visit'
                    WHEN so.pay_time IS NOT NULL      THEN 'pay' 
                END), 
                0
            ) * 100, 
            2
        ), 
        '%'

