-- tire test: http://tire.etsycorp.com/test/Hfp03TRbkCu27eIpkplS

-- sample from first 3 pages & post blending/semrel filter stage
create or replace table `etsy-search-ml-dev.search.yzhang_rel_metrics_in_tire_reqs` as (
  with control_requests as (
    select
      response.mmxRequestUUID,
      COALESCE((SELECT NULLIF(query, '') FROM UNNEST(request.filter.query.translations) WHERE language = 'en'), NULLIF(request.query, '')) query,
      OrganicRequestMetadata.candidateSources,
      (SELECT COUNTIF(stage='POST_SEM_REL_FILTER') FROM UNNEST(OrganicRequestMetadata.candidateSources)) nPostSemrelSources
    FROM `etsy-searchinfra-gke-dev.thrift_mmx_listingsv2search_search.rpc_logs*`
    WHERE request.options.cacheBucketId LIKE "replay-test/%/Hfp03TRbkCu27eIpkplS/%|control|live|web"
    and DATE(queryTime) = "2025-10-14"
    and EXISTS (
      SELECT 1 
      FROM UNNEST(OrganicRequestMetadata.candidateSources) AS candidateSource
      WHERE candidateSource.stage is not null
    )
  ),
  control_results as (
    select 
      "control" as variantName,
      mmxRequestUUID, query,
      ARRAY_CONCAT(
        ARRAY(
          SELECT STRUCT( listing_id AS listingId, idx AS rank, "organic_mo" AS resultType)
          FROM UNNEST(candidateSources) cs, UNNEST(cs.listingIds) AS listing_id WITH OFFSET idx
          WHERE cs.stage = "MO_LASTPASS"
          AND idx < 144
        ),
        ARRAY(
          SELECT STRUCT( listing_id AS listingId, idx AS rank, "organic_blend" AS resultType)
          FROM UNNEST(candidateSources) cs, UNNEST(cs.listingIds) AS listing_id WITH OFFSET idx
          WHERE cs.stage = IF(nPostSemrelSources>0, "POST_SEM_REL_FILTER", "POST_BORDA")
          ORDER BY RAND()
          LIMIT 250
        )
    ) listingSamples
    from control_requests
  ),
  control_results_flat as (
    SELECT * EXCEPT (listingSamples)
    FROM control_results, UNNEST(control_results.listingSamples) listingSample
  ),
  test_requests as (
    select
      response.mmxRequestUUID,
      COALESCE((SELECT NULLIF(query, '') FROM UNNEST(request.filter.query.translations) WHERE language = 'en'), NULLIF(request.query, '')) query,
      OrganicRequestMetadata.candidateSources,
      (SELECT COUNTIF(stage='POST_SEM_REL_FILTER') FROM UNNEST(OrganicRequestMetadata.candidateSources)) nPostSemrelSources
    FROM `etsy-searchinfra-gke-dev.thrift_mmx_listingsv2search_search.rpc_logs*`
    WHERE request.options.cacheBucketId LIKE "replay-test/%/Hfp03TRbkCu27eIpkplS/%|variant|live|web"
    and DATE(queryTime) = "2025-10-14"
    and EXISTS (
      SELECT 1 
      FROM UNNEST(OrganicRequestMetadata.candidateSources) AS candidateSource
      WHERE candidateSource.stage is not null
    )
  ),
  test_results as (
    select 
      "test" as variantName,
      mmxRequestUUID, query,
      ARRAY_CONCAT(
        ARRAY(
          SELECT STRUCT( listing_id AS listingId, idx AS rank, "organic_mo" AS resultType)
          FROM UNNEST(candidateSources) cs, UNNEST(cs.listingIds) AS listing_id WITH OFFSET idx
          WHERE cs.stage = "MO_LASTPASS"
          AND idx < 144
        ),
        ARRAY(
          SELECT STRUCT( listing_id AS listingId, idx AS rank, "organic_blend" AS resultType)
          FROM UNNEST(candidateSources) cs, UNNEST(cs.listingIds) AS listing_id WITH OFFSET idx
          WHERE cs.stage = IF(nPostSemrelSources>0, "POST_SEM_REL_FILTER", "POST_BORDA")
          ORDER BY RAND()
          LIMIT 250
        )
    ) listingSamples
    from test_requests
  ),
  test_results_flat as (
    SELECT * EXCEPT (listingSamples)
    FROM test_results, UNNEST(test_results.listingSamples) listingSample
  )
  select distinct * from control_results_flat
  union all  
  select distinct * from test_results_flat
)


-- get student model scores for distinct (query, listingId)
create or replace table `etsy-search-ml-dev.search.yzhang_rel_metrics_in_tire_relclass` as (
    WITH rpc AS (
        SELECT
            response.mmxRequestUUID,
            request.query AS query,
            OrganicRequestMetadata.candidateSources AS candidateSources,
            response.semanticRelevanceModelInfo.modelSetName as sem_rel_modelset_name,
            response.semanticRelevanceScores AS semanticRelevanceScores,
        FROM `etsy-searchinfra-gke-dev.thrift_mmx_listingsv2search_search.rpc_logs*`
        WHERE request.options.cacheBucketId LIKE "replay-test/%/Hfp03TRbkCu27eIpkplS/%|live|web"
        and DATE(queryTime) = "2025-10-14"
    )
    SELECT distinct
        rpc.query,
        sem_rel_modelset_name,
        sem_rel_score.listingId AS listingId,
        sem_rel_score.relevanceClass AS rel_class
    FROM rpc, UNNEST(semanticRelevanceScores) sem_rel_score
)


create or replace table `etsy-search-ml-dev.search.yzhang_rel_metrics_in_tire_merged` as (
    select 
        reqs.*,
        rel.sem_rel_modelset_name,
        rel.rel_class
    from `etsy-search-ml-dev.search.yzhang_rel_metrics_in_tire_reqs` reqs
    left join `etsy-search-ml-dev.search.yzhang_rel_metrics_in_tire_relclass` rel
    using (query, listingId)
    where sem_rel_modelset_name = "semrel-student-v3"
    and rel_class is not null
)


with count_listings as (
  select variantName, mmxRequestUUID, count(*) as cnt
  from `etsy-search-ml-dev.search.yzhang_rel_metrics_in_tire_merged`
  where resultType = "organic_mo"
  group by variantName, mmxRequestUUID
),
valid_requests as (
  select variantName, mmxRequestUUID
  from count_listings
  where cnt = 144
),
page1_res as (
  select 
    variantName, mmxRequestUUID, 
    sum(IF(rel_class = "IRR", 1, 0)) n_semrel,
    count(*) as n_total
  from `etsy-search-ml-dev.search.yzhang_rel_metrics_in_tire_merged`
  where mmxRequestUUID is not null
  and resultType = "organic_mo"
  and rank < 48
  group by variantName, mmxRequestUUID
),
valid_page1_res as (
  select * 
  from page1_res
  join valid_requests
  using(variantName, mmxRequestUUID)
),
valid_page1_pct as (
  select variantName, mmxRequestUUID, n_semrel / n_total as pct_semrel
  from valid_page1_res
)
select variantName, avg(pct_semrel), count(*) as n_requests
from valid_page1_pct
group by variantName