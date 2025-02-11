WITH one_day_requests AS (
    SELECT DISTINCT
        response.mmxRequestUUID as mmxRequestUUID,
        COALESCE((SELECT NULLIF(query, '') FROM UNNEST(request.filter.query.translations) WHERE language = 'en'), NULLIF(request.query, '')) query,
        request.options.userCountry userCountry,
    FROM `etsy-searchinfra-gke-prod-2.thrift_mmx_listingsv2search_search.rpc_logs_*`
    WHERE DATE(queryTime) = date('2025-02-08')
    AND request.OPTIONS.cacheBucketId LIKE "live%"
    AND request.options.csrOrganic
    AND request.options.searchPlacement IN ('wsg', 'allsr', 'wmg')
    AND request.query <> ''
    AND request.offset = 0
    AND request.options.interleavingConfig IS NULL
    AND NOT EXISTS (
      SELECT * FROM UNNEST(request.context)
      WHERE key = "req_source" AND value = "bot"
    )
)
select count(distinct mmxRequestUUID)
from one_day_requests
where userCountry = "US"
-- 20821436 distinct requests
-- 10619882 (51%) US (userCountry = US)
-- 10201554 (49%) non-US



WITH one_day_requests AS (
    SELECT DISTINCT
        response.mmxRequestUUID as mmxRequestUUID,
        COALESCE((SELECT NULLIF(query, '') FROM UNNEST(request.filter.query.translations) WHERE language = 'en'), NULLIF(request.query, '')) query,
        request.options.userCountry userCountry,
    FROM `etsy-searchinfra-gke-prod-2.thrift_mmx_listingsv2search_search.rpc_logs_*`
    WHERE DATE(queryTime) = date('2025-02-08')
    AND request.OPTIONS.cacheBucketId LIKE "live%"
    AND request.options.csrOrganic
    AND request.options.searchPlacement IN ('wsg', 'allsr', 'wmg')
    AND request.query <> ''
    AND request.offset = 0
    AND request.options.interleavingConfig IS NULL
    AND NOT EXISTS (
      SELECT * FROM UNNEST(request.context)
      WHERE key = "req_source" AND value = "bot"
    )
),
qlm AS (
  select query_raw query, bin as queryBin 
  from `etsy-batchjobs-prod.snapshots.query_level_metrics_raw`
  where _date = date('2025-02-08')
),
qis AS (
  SELECT query_raw query,
  CASE 
    WHEN class_id = 0 THEN 'broad' 
    WHEN class_id = 1 THEN 'direct_unspecified'
    WHEN class_id = 2 THEN 'direct_specified' 
  END AS qisClass
  FROM `etsy-search-ml-prod.mission_understanding.qis_scores`
),
qisv2 AS (
  SELECT query,
  CASE 
    WHEN prediction = 0 THEN 'broad' 
    WHEN prediction = 1 THEN 'direct_unspecified'
    WHEN prediction = 2 THEN 'direct_specified' 
  END AS qisClassV2
  FROM `etsy-search-ml-prod.mission_understanding.qis_scores_v2`
),
merged as (
    select 
        r.*, queryBin, qisClass, qisClassV2,
        REGEXP_CONTAINS(query, '(\?i)\\bgift|\\bfor (\\bhim|\\bher|\\bmom|\\bdad|\\bmother|\\bfather|\\bdaughter|\\bson|\\bwife|\\bhusband|\\bpartner|\\baunt|\\buncle|\\bniece|\\bnephew|\\bfiance|\\bcousin|\\bin law|\\bboyfriend|\\bgirlfriend|\\bgrand|\\bfriend|\\bbest friend)') isGift,
    from one_day_requests r
    left join qlm using (query)
    left join qis using (query)
    left join qisv2 using (query)
)
select queryBin, count(*) as cnt
from merged
group by queryBin
order by queryBin



SELECT top_category, count(*) as cnt 
FROM `etsy-data-warehouse-prod.listing_mart.active_listing_vw`
group by top_category
order by cnt desc
