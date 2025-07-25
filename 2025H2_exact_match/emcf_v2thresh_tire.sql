with requests as (
  select
    response.mmxRequestUUID,
    COALESCE((SELECT NULLIF(query, '') FROM UNNEST(request.filter.query.translations) WHERE language = 'en'), NULLIF(request.query, '')) query,
    OrganicRequestMetadata.candidateSources,
    (SELECT COUNTIF(stage='POST_SEM_REL_FILTER') FROM UNNEST(OrganicRequestMetadata.candidateSources)) nPostSemrelSources
  FROM `etsy-searchinfra-gke-dev.thrift_mmx_listingsv2search_search.rpc_logs*`
  WHERE request.options.cacheBucketId LIKE "replay-test/%/BdtHg63mrNC5aA4xloUk/%|control|live|web"
  and DATE(queryTime) = "2025-07-25"
  and EXISTS (
    SELECT 1 
    FROM UNNEST(OrganicRequestMetadata.candidateSources) AS candidateSource
    WHERE candidateSource.stage is not null
  )
),
results as (
  select 
    mmxRequestUUID,
    query,
    ARRAY_LENGTH(ARRAY(
        SELECT STRUCT( listing_id AS listingId, idx AS bordaRank )
        FROM UNNEST(candidateSources) cs, UNNEST(cs.listingIds) AS listing_id WITH OFFSET idx
        WHERE cs.stage = IF(nPostSemrelSources>0, "POST_SEM_REL_FILTER", "POST_BORDA")
    )) n_candidates
  from requests
)
select max(n_candidates), avg(n_candidates) from results
-- control: 2358, 1511.17
-- test1: 2182	549.35
