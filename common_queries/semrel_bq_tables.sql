--- Join tables to get classes for samples
SELECT
  mmxRequestUUID,
  guid,
  visit_id,
  r.resultType,
  query,
  listingId,
  r.date,
  platform,
  userLanguage,
  userCountry,
  CASE 
    WHEN classId = 1 THEN 'Irrelevant' 
    WHEN classId = 2 THEN 'Partial'
    WHEN classId = 3 THEN 'Relevant' 
  END AS semrelClass,
  rankingRank
FROM `etsy-data-warehouse-prod.search.sem_rel_hydrated_daily_requests` r
JOIN `etsy-data-warehouse-prod.search.sem_rel_query_listing_metrics` USING (tableUUID)
WHERE query = "cat"
AND modelName = "v3-finetuned-llama-8b" 
-- AND modelName = "v2-deberta-v3-large-tad"
AND pageNum = 1
order by r.date, visit_id, guid, rankingRank



--- Check %irrelevant for an experiment
with semrel_results as (
    SELECT variantName, guid, query, platform, userLanguage, listingId, classId
    FROM `etsy-data-warehouse-prod.search.sem_rel_hydrated_daily_requests_per_experiment` s
    left JOIN `etsy-data-warehouse-prod.search.sem_rel_query_listing_metrics_per_experiment` m
    USING (tableUUID, date)
    WHERE configFlag = "ranking/isearch.exact_match_unify_with_v2_loc"
    -- AND s.date = "2025-08-01"
    AND pageNum is not null
    -- AND modelName = "v2-deberta-v3-large-tad"
    AND modelName = "v3-finetuned-llama-8b"
),
agg_results as (
    select variantName, guid, sum(if(classId=1, 1, 0)) as n_irrelevant, count(*) as n_total
    from semrel_results
    group by variantName, guid
)
select variantName, avg(n_irrelevant / n_total)
from agg_results
group by variantName