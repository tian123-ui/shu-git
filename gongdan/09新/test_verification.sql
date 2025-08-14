-- 店内路径看板分层与不分层数据一致性测试SQL
-- 测试周期: 2025-01-01 至 2025-01-30
-- 生成时间: 2025-08-03 21:48:35
-- 使用实际字段名: visitor_count(总访客数), page_views(浏览量)

-- =============================================
-- TC001: 总访客数一致性验证
-- =============================================
-- 分层方式（使用实际字段名）
SELECT SUM(visitor_count) AS 分层总访客数
FROM ads_page_visit_rank
WHERE time_dimension BETWEEN '2025-01-01' AND '2025-01-30';

-- 不分层方式
SELECT COUNT(DISTINCT visitor_id) AS 不分层总访客数
FROM ods_page_behavior
WHERE stat_date BETWEEN '2025-01-01' AND '2025-01-30';

-- =============================================
-- TC002: 总浏览量一致性验证
-- =============================================
-- 分层方式（使用实际字段名）
SELECT SUM(page_views) AS 分层总浏览量
FROM ads_page_visit_rank
WHERE time_dimension BETWEEN '2025-01-01' AND '2025-01-30';

-- 不分层方式
SELECT COUNT(*) AS 不分层总浏览量
FROM ods_page_behavior
WHERE stat_date BETWEEN '2025-01-01' AND '2025-01-30';
