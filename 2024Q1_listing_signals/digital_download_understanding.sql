SELECT is_digital, count(*) as cnt
FROM `etsy-data-warehouse-prod.rollups.query_level_metrics_raw`
group by is_digital
-- 821,217,709 raw queries
-- 789322656 non digital
-- 31895053 (3.9%) digital

SELECT query_raw, query_normalized
FROM `etsy-data-warehouse-prod.rollups.query_level_metrics_raw`
where is_digital = 0
and lower(query_raw) like "%plan%"
-- 1320269 pdf
-- 7286987 plan
-- 2388671 stl
-- 497967 overlay
-- 9555877 pattern
-- 3690968 template
-- 8029912 design
-- 611236 emote
-- 5627255 poster
-- 889751 font
-- 16174782 print
-- 234194 svg
-- 132401 png
-- 744292 chart