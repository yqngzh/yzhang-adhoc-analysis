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



---- student score from rpc logs
WITH rpc AS (
  SELECT
      response.mmxRequestUUID,
      request.query AS query,
      request.context AS context,
      OrganicRequestMetadata.candidateSources AS candidateSources,
      response.semanticRelevanceModelInfo.modelSetName as sem_rel_modelset_name,
      response.semanticRelevanceScores AS semanticRelevanceScores,
  FROM `etsy-searchinfra-gke-prod-2.thrift_mmx_listingsv2search_search.rpc_logs_*`
  WHERE 
      queryTime BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE)  -- seconds
    AND CURRENT_TIMESTAMP() 
      AND request.query <> ''
      AND request.options.csrOrganic
      AND request.options.interleavingConfig IS NULL
      AND OrganicRequestMetadata IS NOT NULL
)
SELECT
    rpc.mmxRequestUUID,
    rpc.query,
    sem_rel_modelset_name,
    sem_rel_score.listingId AS listing_id,
    sem_rel_score.candidateSource AS source,
    sem_rel_score.relevanceClass AS rel_class
FROM rpc,
  UNNEST(semanticRelevanceScores) sem_rel_score
LIMIT 10000



--- semrel dashboard diagnosis
select date, resultType, count(*)
from `etsy-data-warehouse-prod.search.sem_rel_hydrated_daily_requests_per_experiment`
group by date, resultType
order by date desc, resultType

select date, resultType, count(*)
from `etsy-data-warehouse-prod.search.sem_rel_query_listing_metrics_per_experiment`
group by date, resultType
order by date desc, resultType

select date, resultType, count(*)
from `etsy-data-warehouse-prod.search.sem_rel_requests_metrics_per_experiment`
group by date, resultType
order by date desc, resultType

delete from `etsy-data-warehouse-prod.search.sem_rel_hydrated_daily_requests_per_experiment`
where date between '2025-09-04' and "2025-09-06" 
