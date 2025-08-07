--  Note: this script is not used in experiment results
--  but instead we are revising based on latest understanding of TIRE practice
--  to prepare for future use


--------   Organic Metadata level  --------
create or replace table `etsy-search-ml-dev.search.yzhang_em_tire_om_y5bJzPoWGz9z8ehdIf1w` as (
  with requests as (
    SELECT
      a.response.mmxRequestUUID,
      COALESCE((SELECT NULLIF(a.query, '') FROM UNNEST(a.request.filter.query.translations) WHERE language = 'en'), NULLIF(a.request.query, '')) query,
      a.OrganicRequestMetadata.candidateSources,
      c.tireRequestContext.variant variantName
    FROM `etsy-searchinfra-gke-dev.thrift_mmx_listingsv2search_search.rpc_logs*` a
    JOIN `etsy-searchinfra-gke-dev.thrift_tire_searchwithads_rpc_logs.rpc_logs*` c
    ON a.response.mmxRequestUUID = c.response.preserved.organicResults.mmxRequestUUID
    AND c.tireRequestContext.tireTestv2Id = "y5bJzPoWGz9z8ehdIf1w"
    WHERE DATE(a.queryTime) = "2025-08-04" AND DATE(c.queryTime) = "2025-08-04"
    AND EXISTS (
      SELECT 1
      FROM UNNEST(a.OrganicRequestMetadata.candidateSources) AS cs
      WHERE cs.stage IS NOT NULL
    )
  ),
  organic_results as (
    select 
      variantName, mmxRequestUUID, query,
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
    from requests
  ),
  organic_results_flat as (
    SELECT * EXCEPT (listingSamples)
    FROM organic_results, UNNEST(organic_results.listingSamples) listingSample
  ),
  lfb as (
    select 
      key as listingId,
      IFNULL(
          COALESCE(NULLIF(verticaListings_title, ''), NULLIF(verticaListingTranslations_machineTranslatedEnglishTitle, '')),
          ""
      ) listingTitle,
      IFNULL(verticaSellerBasics_shopName, "") listingShopName,
      IFNULL(listingLlmFeatures_llmHeroImageDescription, "") listingHeroImageCaption,
      IFNULL((SELECT STRING_AGG(element, ', ') FROM UNNEST(descNgrams_ngrams.list)), "") listingDescNgrams,
    from `etsy-ml-systems-prod.feature_bank_v2.listing_feature_bank_most_recent`
  )
  select * 
  from organic_results_flat
  left join lfb using (listingId)
  where query is not null and query != ""
)


with tmp as (
  select distinct query, listingId, listingTitle, listingShopName, listingHeroImageCaption, listingDescNgrams
  from `etsy-search-ml-dev.search.yzhang_em_tire_y5bJzPoWGz9z8ehdIf1w`
)
select count(*) from tmp


create or replace table `etsy-search-ml-dev.search.yzhang_em_tire_results_y5bJzPoWGz9z8ehdIf1w` as (
  select ori.*, semrelLabel
  from `etsy-search-ml-dev.search.yzhang_em_tire_y5bJzPoWGz9z8ehdIf1w` ori
  left join `etsy-search-ml-dev.search.semrel_adhoc_yzhang_em_tire_y5bJzPoWGz9z8ehdIf1w`
  using (query, listingId)
)


-- organic @48 or @24
with count_listings as (
  select variantName, mmxRequestUUID, count(*) as cnt
  from `etsy-search-ml-dev.search.yzhang_em_tire_results_y5bJzPoWGz9z8ehdIf1w`
  where resultType = "organic_mo"
  group by variantName, mmxRequestUUID
),
valid_requests as (
  select variantName, mmxRequestUUID
  from count_listings
  where cnt = 144
),
page1_irrelevance as (
  select 
    variantName, mmxRequestUUID, 
    sum(IF(semrelLabel = "not_relevant", 1, 0)) n_irrelevant, 
    count(*) as n_total
  from `etsy-search-ml-dev.search.yzhang_em_tire_results_y5bJzPoWGz9z8ehdIf1w`
  where mmxRequestUUID is not null
  and semrelLabel is not null
  and resultType = "organic_mo"
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
  from `etsy-search-ml-dev.search.yzhang_em_tire_results_y5bJzPoWGz9z8ehdIf1w`
  where resultType = "organic_blend"
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
  from `etsy-search-ml-dev.search.yzhang_em_tire_results_y5bJzPoWGz9z8ehdIf1w`
  where mmxRequestUUID is not null
  and semrelLabel is not null
  and resultType = "organic_blend"
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


-- candidate size change
-- modifiy






--------   For 1st page after JOINS  --------
-- TODO: get query





-- 1st page price diff
with requests as (
  select
    response.mmxRequestUUID,
    COALESCE((SELECT NULLIF(query, '') FROM UNNEST(request.filter.query.translations) WHERE language = 'en'), NULLIF(request.query, '')) query,
    OrganicRequestMetadata.candidateSources,
    (SELECT COUNTIF(stage='POST_SEM_REL_FILTER') FROM UNNEST(OrganicRequestMetadata.candidateSources)) nPostSemrelSources
  FROM `etsy-searchinfra-gke-dev.thrift_mmx_listingsv2search_search.rpc_logs*`
  WHERE request.options.cacheBucketId LIKE "replay-test/%/y5bJzPoWGz9z8ehdIf1w/%|test1|live|web"
  and DATE(queryTime) = "2025-08-04"
  and EXISTS (
    SELECT 1 
    FROM UNNEST(OrganicRequestMetadata.candidateSources) AS candidateSource
    WHERE candidateSource.stage is not null
  )
),
results as (
  select 
    mmxRequestUUID, query,
    ARRAY(
        SELECT STRUCT( listing_id AS listingId, idx AS rank, 1 AS pageNum)
        FROM UNNEST(candidateSources) cs, UNNEST(cs.listingIds) AS listing_id WITH OFFSET idx
        WHERE cs.stage = "MO_LASTPASS"
        AND idx < 48
        -- AND idx < 10
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
    -- activeListingBasics_priceUsd as price
    (select value from unnest(listingWeb_price.key_value) where key = 'US') price
  FROM `etsy-ml-systems-prod.feature_bank_v2.listing_feature_bank_most_recent`
),
full_results as (
  SELECT r.*, price
  from results_flat r
  left join lfb using (listingId)
),
agg_first as (
  select mmxRequestUUID, avg(price) as avg_price
  from full_results
  group by  mmxRequestUUID
)
select avg(avg_price)
from agg_first

