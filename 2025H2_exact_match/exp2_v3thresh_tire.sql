-- create table
create or replace table `etsy-search-ml-dev.search.yzhang_emv3student_tire_uyzy4il8NyF001sTPADa` as (
  with requests as (
    SELECT
      a.response.mmxRequestUUID,
      COALESCE((SELECT NULLIF(query, '') FROM UNNEST(a.request.filter.query.translations) WHERE language = 'en'), NULLIF(a.request.query, '')) query,
      a.OrganicRequestMetadata.candidateSources,
      c.tireRequestContext.variant variantName,
      (SELECT COUNTIF(stage='POST_SEM_REL_FILTER') FROM UNNEST(OrganicRequestMetadata.candidateSources)) nPostSemrelSources
    FROM `etsy-searchinfra-gke-dev.thrift_mmx_listingsv2search_search.rpc_logs*` a
    JOIN `etsy-searchinfra-gke-dev.thrift_tire_listingsv2search_search.rpc_logs*` c
    ON (
      a.response.mmxRequestUUID = c.response.mmxRequestUUID
      AND c.tireRequestContext.tireTestv2Id = "uyzy4il8NyF001sTPADa"
      AND a.request.options.cacheBucketId LIKE "replay-test/%/uyzy4il8NyF001sTPADa/%|live|web"
    )
    WHERE DATE(a.queryTime) = "2025-08-24" AND DATE(c.queryTime) = "2025-08-24"
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
      (select value from unnest(listingWeb_price.key_value) where key = 'US') price
    from `etsy-ml-systems-prod.feature_bank_v2.listing_feature_bank_most_recent`
  )
  select * 
  from organic_results_flat
  left join lfb using (listingId)
  where query is not null and query != ""
)

-- check how many query listing pairs are in table
with tmp as (
  select distinct query, listingId, listingTitle, listingShopName, listingHeroImageCaption, listingDescNgrams
  from `etsy-search-ml-dev.search.yzhang_emv3student_tire_uyzy4il8NyF001sTPADa`
)
select count(*) from tmp

-- run semrel adhoc teacher inference


-- start analysis
create or replace table `etsy-search-ml-dev.search.yzhang_emv3student_tire_results_uyzy4il8NyF001sTPADa` as (
  select ori.*, semrelLabel
  from `etsy-search-ml-dev.search.yzhang_emv3student_tire_uyzy4il8NyF001sTPADa` ori
  left join `etsy-search-ml-dev.search.semrel_adhoc_yzhang_emv3student_tire_uyzy4il8NyF001sTPADa`
  using (query, listingId)
)

-- @48
with count_listings as (
  select variantName, mmxRequestUUID, count(*) as cnt
  from `etsy-search-ml-dev.search.yzhang_emv3student_tire_results_uyzy4il8NyF001sTPADa`
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
    sum(IF(semrelLabel = "not_relevant", 1, 0)) n_semrel, -- change to relevant
    avg(price) as avg_price,
    count(*) as n_total
  from `etsy-search-ml-dev.search.yzhang_emv3student_tire_results_uyzy4il8NyF001sTPADa`
  where mmxRequestUUID is not null
  and semrelLabel is not null
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
-- price @48
-- select variantName, avg(avg_price), count(*) as n_requests
-- from valid_page1_res
-- group by variantName


-- blending
with count_listings as (
  select variantName, mmxRequestUUID, count(*) as cnt
  from `etsy-search-ml-dev.search.yzhang_emv3student_tire_results_uyzy4il8NyF001sTPADa`
  where resultType = "organic_blend"
  group by variantName, mmxRequestUUID
),
valid_requests as (
  select variantName, mmxRequestUUID
  from count_listings
  where cnt = 250
),
blend_res as (
  select 
    variantName, mmxRequestUUID, 
    sum(IF(semrelLabel = "not_relevant", 1, 0)) n_semrel, -- change to relevant
    count(*) as n_total
  from `etsy-search-ml-dev.search.yzhang_emv3student_tire_results_uyzy4il8NyF001sTPADa`
  where mmxRequestUUID is not null
  and semrelLabel is not null
  and resultType = "organic_blend"
  group by variantName, mmxRequestUUID
),
valid_blend_res as (
  select * 
  from blend_res
  join valid_requests
  using(variantName, mmxRequestUUID)
),
valid_blend_pct as (
  select variantName, mmxRequestUUID, n_semrel / n_total as pct_semrel
  from valid_blend_res
)
select variantName, avg(pct_semrel), count(*) as n_requests
from valid_blend_pct
group by variantName


-- candidate size change
-- https://github.com/etsy/search-retrieval-tools/blob/main/bigquery/new_listing_stats_tire_blender.sql
