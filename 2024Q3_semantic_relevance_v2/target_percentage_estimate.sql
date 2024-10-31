--- SQL for baseline dashboard
--- https://github.com/etsy/LookerDataAccessControlled/blob/master/search_semantic_relevance_requests.bq.view.lkml


-- On 10/29, by V2 teacher, percentages on page 1-3 
with request_attributes as (
    select
        date,
        modelName,
        mmxRequestUUID,
        guid,
        COALESCE(r.resultType, 'organic') AS resultType,
        MAX(userLanguage) AS userLanguage,
        MAX(userCountry) AS userCountry,
        MAX(LOWER(query)) AS query,
        MAX(platform) AS platform,
        MAX(queryBin) AS queryBin,
        MAX(qisClass) AS qisClass,
        MAX(SPLIT(queryTaxo, '.')[OFFSET(0)]) AS queryTaxo,

        -- PG 1-3 METRICS
        COUNT(CASE WHEN pageNum BETWEEN 1 AND 3 THEN 1 END) AS pg1_3_listings,
        COUNT(CASE WHEN pageNum BETWEEN 1 AND 3 AND classId = IF(modelName = 'bert-cern-l24-h1024-a16', 4, 3) THEN 1 END) AS pg1_3_relevant_listings,
        COUNT(CASE WHEN pageNum BETWEEN 1 AND 3 AND (classId = IF(modelName = 'bert-cern-l24-h1024-a16', 3, 2) OR classId = 2) THEN 1 END) AS pg1_3_partial_rel_listings,
        COUNT(CASE WHEN pageNum BETWEEN 1 AND 3 AND classId = 1 THEN 1 END) AS pg1_3_irrelevant_listings,
    FROM `etsy-data-warehouse-prod.search.sem_rel_hydrated_daily_requests` r
    LEFT JOIN `etsy-data-warehouse-prod.search.sem_rel_query_listing_metrics` ql
    USING (date, tableUUID)
    WHERE date = date('2024-10-29')
    GROUP BY all
    HAVING COUNT(CASE WHEN pageNum = 1 THEN 1 END) > 0
),
result_data as (
  SELECT 
    *,
    SAFE_DIVIDE(pg1_3_relevant_listings, pg1_3_listings) as percent_relevant,
    SAFE_DIVIDE(pg1_3_partial_rel_listings, pg1_3_listings) as percent_partial,
    SAFE_DIVIDE(pg1_3_irrelevant_listings, pg1_3_listings) as percent_irrelevant,
  FROM request_attributes
)
select 
  platform, 
  AVG(percent_relevant) as avg_percent_relevant,
  AVG(percent_partial) as avg_percent_partial,
  AVG(percent_irrelevant) as avg_percent_irrelevant
from result_data
where modelName = 'v2-deberta-v3-large-tad'
and resultType = 'organic'
and userCountry = 'US'
and qisClass = 'direct_specified'
group by platform



-- By V2 teacher, pre-borda and post-borda
with request_attributes as (
    select
        date,
        modelName,
        mmxRequestUUID,
        guid,
        COALESCE(r.resultType, 'organic') AS resultType,
        MAX(userLanguage) AS userLanguage,
        MAX(userCountry) AS userCountry,
        MAX(LOWER(query)) AS query,
        MAX(platform) AS platform,
        MAX(queryBin) AS queryBin,
        MAX(qisClass) AS qisClass,
        MAX(SPLIT(queryTaxo, '.')[OFFSET(0)]) AS queryTaxo,

        -- POST-BORDA METRICS
        COUNT(CASE WHEN bordaRank IS NOT NULL THEN 1 END) AS post_borda_listings,
        COUNT(CASE WHEN bordaRank IS NOT NULL AND classId = IF(modelName = 'bert-cern-l24-h1024-a16', 4, 3) THEN 1 END) AS post_borda_relevant_listings,
        COUNT(CASE WHEN bordaRank IS NOT NULL AND (classId = IF(modelName = 'bert-cern-l24-h1024-a16', 3, 2) OR classId = 2) THEN 1 END) AS post_borda_partial_rel_listings,
        COUNT(CASE WHEN bordaRank IS NOT NULL AND classId = 1 THEN 1 END) AS post_borda_irrelevant_listings,

        -- PRE-BORDA METRICS
        COUNT(CASE WHEN retrievalRank IS NOT NULL THEN 1 END) AS pre_borda_listings,
        COUNT(CASE WHEN retrievalRank IS NOT NULL AND classId = IF(modelName = 'bert-cern-l24-h1024-a16', 4, 3) THEN 1 END) AS pre_borda_relevant_listings,
        COUNT(CASE WHEN retrievalRank IS NOT NULL AND (classId = IF(modelName = 'bert-cern-l24-h1024-a16', 3, 2) OR classId = 2) THEN 1 END) AS pre_borda_partial_rel_listings,
        COUNT(CASE WHEN retrievalRank IS NOT NULL AND classId = 1 THEN 1 END) AS pre_borda_irrelevant_listings,
    FROM `etsy-data-warehouse-prod.search.sem_rel_hydrated_daily_requests` r
    LEFT JOIN `etsy-data-warehouse-prod.search.sem_rel_query_listing_metrics` ql
    USING (date, tableUUID)
    WHERE date = date('2024-10-29')
    GROUP BY all
    HAVING COUNT(CASE WHEN pageNum = 1 THEN 1 END) > 0
),
result_data as (
  SELECT 
    *,
    SAFE_DIVIDE(post_borda_relevant_listings, post_borda_listings) as post_borda_pct_relevant,
    SAFE_DIVIDE(post_borda_partial_rel_listings, post_borda_listings) as post_borda_pct_partial,
    SAFE_DIVIDE(post_borda_irrelevant_listings, post_borda_listings) as post_borda_pct_irrelevant,
    SAFE_DIVIDE(pre_borda_relevant_listings, pre_borda_listings) as pre_borda_pct_relevant,
    SAFE_DIVIDE(pre_borda_partial_rel_listings, pre_borda_listings) as pre_borda_pct_partial,
    SAFE_DIVIDE(pre_borda_irrelevant_listings, pre_borda_listings) as pre_borda_pct_irrelevant,
  FROM request_attributes
)
select 
  platform, 
  AVG(post_borda_pct_relevant) as avg_percent_relevant,
  AVG(post_borda_pct_partial) as avg_percent_partial,
  AVG(post_borda_pct_irrelevant) as avg_percent_irrelevant
from result_data
where modelName = 'v2-deberta-v3-large-tad'
and resultType = 'organic'
and userCountry = 'US'
group by platform