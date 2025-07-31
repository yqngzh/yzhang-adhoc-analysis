-- what to change: tire ID, query date
create or replace table `etsy-search-ml-dev.search.yzhang_em_tire_8sffBs5zGuhhLfXcaLYK` as (
  with control_requests as (
    select
      response.mmxRequestUUID,
      COALESCE((SELECT NULLIF(query, '') FROM UNNEST(request.filter.query.translations) WHERE language = 'en'), NULLIF(request.query, '')) query,
      OrganicRequestMetadata.candidateSources,
      (SELECT COUNTIF(stage='POST_SEM_REL_FILTER') FROM UNNEST(OrganicRequestMetadata.candidateSources)) nPostSemrelSources
    FROM `etsy-searchinfra-gke-dev.thrift_mmx_listingsv2search_search.rpc_logs*`
    WHERE request.options.cacheBucketId LIKE "replay-test/%/8sffBs5zGuhhLfXcaLYK/%|control|live|web"
    and DATE(queryTime) = "2025-07-31"
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
          SELECT STRUCT( listing_id AS listingId, idx AS rank, 1 AS pageNum)
          FROM UNNEST(candidateSources) cs, UNNEST(cs.listingIds) AS listing_id WITH OFFSET idx
          WHERE cs.stage = "MO_LASTPASS"
          AND idx < 48
        ),
        ARRAY(
          SELECT STRUCT( listing_id AS listingId, idx AS rank, -1 AS pageNum)
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
    WHERE request.options.cacheBucketId LIKE "replay-test/%/8sffBs5zGuhhLfXcaLYK/%|test1|live|web"
    and DATE(queryTime) = "2025-07-31"
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
          SELECT STRUCT( listing_id AS listingId, idx AS rank, 1 AS pageNum)
          FROM UNNEST(candidateSources) cs, UNNEST(cs.listingIds) AS listing_id WITH OFFSET idx
          WHERE cs.stage = "MO_LASTPASS"
          AND idx < 48
        ),
        ARRAY(
          SELECT STRUCT( listing_id AS listingId, idx AS rank, -1 AS pageNum)
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
  ),
  merged_results as (
    select distinct * from control_results_flat
    union all 
    select distinct * from test_results_flat
  ),
  lfb as (
    select 
      key as listingId,
      IFNULL(
          COALESCE(NULLIF(verticaListings_title, ''), NULLIF(verticaListingTranslations_machineTranslatedEnglishTitle, '')) listingTitle,,
          ""
      ) listingTitle,
      IFNULL(verticaSellerBasics_shopName, "") listingShopName,
      IFNULL(listingLlmFeatures_llmHeroImageDescription, "") listingHeroImageCaption,
      IFNULL((SELECT STRING_AGG(element, ', ') FROM UNNEST(descNgrams_ngrams.list)), "") listingDescNgrams,
    from `etsy-ml-systems-prod.feature_bank_v2.listing_feature_bank_most_recent`
  )
  select * 
  from merged_results
  left join lfb using (listingId)
)


with tmp as (
  select distinct query, listingId, listingTitle, listingShopName, listingHeroImageCaption, listingDescNgrams
  from `etsy-search-ml-dev.search.yzhang_em_tire_8sffBs5zGuhhLfXcaLYK`
)
select count(*) from tmp


create or replace table `etsy-search-ml-dev.search.yzhang_em_tire_results_8sffBs5zGuhhLfXcaLYK` as (
  select ori.*, semrelLabel
  from `etsy-search-ml-dev.search.yzhang_em_tire_8sffBs5zGuhhLfXcaLYK` ori
  left join `etsy-search-ml-dev.search.semrel_adhoc_yzhang_em_tire_8sffBs5zGuhhLfXcaLYK`
  using (query, listingId)
)


-- @48 or @24
with count_listings as (
  select variantName, mmxRequestUUID, count(*) as cnt
  from `etsy-search-ml-dev.search.yzhang_em_tire_results_8sffBs5zGuhhLfXcaLYK`
  where pageNum = 1
  group by variantName, mmxRequestUUID
),
valid_requests as (
  select variantName, mmxRequestUUID
  from count_listings
  where cnt >= 28
),
page1_irrelevance as (
  select 
    variantName, mmxRequestUUID, 
    sum(IF(semrelLabel = "not_relevant", 1, 0)) n_irrelevant, 
    count(*) as n_total
  from `etsy-search-ml-dev.search.yzhang_em_tire_results_8sffBs5zGuhhLfXcaLYK`
  where mmxRequestUUID is not null
  and semrelLabel is not null
  and pageNum = 1
  and rank < 48
  group by variantName, mmxRequestUUID
),
valid_page1_irrelevance as (
  select * 
  from page1_irrelevance
  join valid_requests
  using(variantName, mmxRequestUUID)
),
valid_page1_pct as (
  select variantName, mmxRequestUUID, n_irrelevant / n_total as pct_irrelevance
  from valid_page1_irrelevance
)
select variantName, avg(pct_irrelevance), count(*) as n_requests
from valid_page1_pct
group by variantName


-- post filtering
with count_listings as (
  select variantName, mmxRequestUUID, count(*) as cnt
  from `etsy-search-ml-dev.search.yzhang_em_tire_results_8sffBs5zGuhhLfXcaLYK`
  where pageNum = -1
  group by variantName, mmxRequestUUID
),
valid_requests as (
  select variantName, mmxRequestUUID
  from count_listings
  where cnt = 250
),
blend_irrelevance as (
  select 
    variantName, mmxRequestUUID, 
    sum(IF(semrelLabel = "not_relevant", 1, 0)) n_irrelevant, 
    count(*) as n_total
  from `etsy-search-ml-dev.search.yzhang_em_tire_results_8sffBs5zGuhhLfXcaLYK`
  where mmxRequestUUID is not null
  and semrelLabel is not null
  and pageNum = -1
  group by variantName, mmxRequestUUID
),
valid_blend_irrelevance as (
  select * 
  from blend_irrelevance
  join valid_requests
  using(variantName, mmxRequestUUID)
),
valid_blend_pct as (
  select variantName, mmxRequestUUID, n_irrelevant / n_total as pct_irrelevance
  from valid_blend_irrelevance
)
select variantName, avg(pct_irrelevance), count(*) as n_requests
from valid_blend_pct
group by variantName


-- price diff
with requests as (
  select
    response.mmxRequestUUID,
    COALESCE((SELECT NULLIF(query, '') FROM UNNEST(request.filter.query.translations) WHERE language = 'en'), NULLIF(request.query, '')) query,
    OrganicRequestMetadata.candidateSources,
    (SELECT COUNTIF(stage='POST_SEM_REL_FILTER') FROM UNNEST(OrganicRequestMetadata.candidateSources)) nPostSemrelSources
  FROM `etsy-searchinfra-gke-dev.thrift_mmx_listingsv2search_search.rpc_logs*`
  WHERE request.options.cacheBucketId LIKE "replay-test/%/eWrU2F6GIkmxzpKxUhDE/%|control|live|web"
  and DATE(queryTime) = "2025-07-29"
  and EXISTS (
    SELECT 1 
    FROM UNNEST(OrganicRequestMetadata.candidateSources) AS candidateSource
    WHERE candidateSource.stage is not null
  )
),
results as (
  select 
    "control" as variantName,
    mmxRequestUUID, query,
    ARRAY(
        SELECT STRUCT( listing_id AS listingId, idx AS rank, 1 AS pageNum)
        FROM UNNEST(candidateSources) cs, UNNEST(cs.listingIds) AS listing_id WITH OFFSET idx
        WHERE cs.stage = "MO_LASTPASS"
        AND idx < 48
    ) listingSamples
  from requests
),
results_flat as (
  SELECT * EXCEPT (listingSamples)
  FROM results, UNNEST(results.listingSamples) listingSample
),
lfb as (
  SELECT 
    key as listingId,
    activeListingBasics_priceUsd as price
  FROM `etsy-ml-systems-prod.feature_bank_v2.listing_feature_bank_most_recent`
),
full_results as (
  SELECT r.*, price
  from results_flat r
  left join lfb using (listingId)
),
agg_first as (
  select variantName, mmxRequestUUID, avg(price) as avg_price
  from full_results
  group by variantName, mmxRequestUUID
)
select variantName, avg(avg_price)
from agg_first
group by variantName



-- candidate size change
-- deprecated, using retrieval tools
with requests as (
  select
    response.mmxRequestUUID,
    COALESCE((SELECT NULLIF(query, '') FROM UNNEST(request.filter.query.translations) WHERE language = 'en'), NULLIF(request.query, '')) query,
    OrganicRequestMetadata.candidateSources,
    (SELECT COUNTIF(stage='POST_SEM_REL_FILTER') FROM UNNEST(OrganicRequestMetadata.candidateSources)) nPostSemrelSources
  FROM `etsy-searchinfra-gke-dev.thrift_mmx_listingsv2search_search.rpc_logs*`
  WHERE request.options.cacheBucketId LIKE "replay-test/%/8sffBs5zGuhhLfXcaLYK/%|control|live|web"
  and DATE(queryTime) = "2025-07-29"
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
SELECT
  percentiles[OFFSET(50)] AS p50_value,
  percentiles[OFFSET(75)] AS p75_value
FROM (
  SELECT APPROX_QUANTILES(n_candidates, 100) AS percentiles
  FROM results
)


-- which query irrelevance changed
with count_listings as (
  select variantName, mmxRequestUUID, query, count(*) as cnt
  from `etsy-search-ml-dev.search.yzhang_em_tire_results_fSPVIkRBlhdl0hce9EIG`
  where pageNum = 1
  group by variantName, mmxRequestUUID, query
),
valid_requests as (
  select variantName, mmxRequestUUID, query
  from count_listings
  where cnt >= 28
),
page1_irrelevance as (
  select 
    variantName, mmxRequestUUID, query,
    sum(IF(semrelLabel = "not_relevant", 1, 0)) n_irrelevant, 
    count(*) as n_total
  from `etsy-search-ml-dev.search.yzhang_em_tire_results_fSPVIkRBlhdl0hce9EIG`
  where mmxRequestUUID is not null
  and semrelLabel is not null
  and query != ""
  and pageNum = 1
  and rank < 48
  group by variantName, mmxRequestUUID, query
),
valid_page1_irrelevance as (
  select * 
  from page1_irrelevance
  join valid_requests
  using(variantName, mmxRequestUUID, query)
),
valid_page1_pct as (
  select variantName, mmxRequestUUID, query, n_irrelevant / n_total as pct_irrelevance
  from valid_page1_irrelevance
)
select variantName, query, avg(pct_irrelevance), count(*) as n_requests
from valid_page1_pct
where pct_irrelevance != 0
group by variantName, query
order by query, variantName