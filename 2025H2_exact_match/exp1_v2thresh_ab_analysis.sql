-- semrel metrics from experiment
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




-- get RPC logs from bucketed events in experiments
declare run_date date default "2025-07-31";

create or replace table `etsy-search-ml-dev.search.yzhang_em_exp1_rpclogs_0731` as (
  with abtest as (
    select distinct variantName, mmxRequestUUID, query as ab_query 
    from `etsy-data-warehouse-prod.search.sem_rel_hydrated_daily_requests_per_experiment`
    where configFlag = "ranking/isearch.exact_match_unify_with_v2_loc"
    and date = run_date
  ),
  rpc as (
    select
      response.mmxRequestUUID,
      COALESCE((SELECT NULLIF(query, '') FROM UNNEST(request.filter.query.translations) WHERE language = 'en'), NULLIF(request.query, '')) query,
      OrganicRequestMetadata.candidateSources,
      (SELECT COUNTIF(stage='POST_SEM_REL_FILTER') FROM UNNEST(OrganicRequestMetadata.candidateSources)) nPostSemrelSources
    FROM `etsy-searchinfra-gke-prod-2.thrift_mmx_listingsv2search_search.rpc_logs_*`
    WHERE DATE(queryTime) = run_date
    and EXISTS (
      SELECT 1 
      FROM UNNEST(OrganicRequestMetadata.candidateSources) AS candidateSource
      WHERE candidateSource.stage is not null
    )
  ),
  abtest_rpc as (
    select 
        variantName,
        mmxRequestUUID,
        ab_query,
        query,
        candidateSources,
        nPostSemrelSources
    from abtest
    join rpc using (mmxRequestUUID)
  ),
  abtest_rpc_results as (
    select 
      variantName, mmxRequestUUID, ab_query, query,
      ARRAY_CONCAT(
        ARRAY(
          SELECT STRUCT( listing_id AS listingId, idx AS rank, 1 AS pageNum)
          FROM UNNEST(candidateSources) cs, UNNEST(cs.listingIds) AS listing_id WITH OFFSET idx
          WHERE cs.stage = "MO_LASTPASS"
          AND idx < 144
        ),
        ARRAY(
          SELECT STRUCT( listing_id AS listingId, idx AS rank, -1 AS pageNum)
          FROM UNNEST(candidateSources) cs, UNNEST(cs.listingIds) AS listing_id WITH OFFSET idx
          WHERE cs.stage = IF(nPostSemrelSources>0, "POST_SEM_REL_FILTER", "POST_BORDA")
          ORDER BY RAND()
          LIMIT 250
        )
      ) listingSamples
    from abtest_rpc
  ),
  abtest_rpc_results_flat as (
    SELECT * EXCEPT (listingSamples)
    FROM abtest_rpc_results, UNNEST(abtest_rpc_results.listingSamples) listingSample
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
  from abtest_rpc_results_flat
  left join lfb using (listingId)
)

with tmp as (
  select distinct query, listingId, listingTitle, listingShopName, listingHeroImageCaption, listingDescNgrams
  from `etsy-search-ml-dev.search.yzhang_em_exp1_rpclogs_0731`
)
select count(*) from tmp



create or replace table `etsy-search-ml-dev.search.yzhang_em_exp1_results_0731` as (
  select ori.*, semrelLabel
  from `etsy-search-ml-dev.search.yzhang_em_exp1_rpclogs_0731` ori
  left join `etsy-search-ml-dev.search.semrel_adhoc_yzhang_em_exp1_rpclogs_0731`
  using (query, listingId)
)


with count_listings as (
  select variantName, mmxRequestUUID, count(*) as cnt
  from `etsy-search-ml-dev.search.yzhang_em_exp1_results_0731`
  where pageNum = 1
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
  from `etsy-search-ml-dev.search.yzhang_em_exp1_results_0731`
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




---- price change
with semrel_results as (
  SELECT variantName, guid, query, listingId
  FROM `etsy-data-warehouse-prod.search.sem_rel_hydrated_daily_requests_per_experiment` s
  left JOIN `etsy-data-warehouse-prod.search.sem_rel_query_listing_metrics_per_experiment` m
  USING (tableUUID, date)
  WHERE configFlag = "ranking/isearch.exact_match_unify_with_v2_loc"
  AND bordaRank is not null
),
lfb as (
  SELECT 
    key as listingId,
    activeListingBasics_priceUsd as price
  FROM `etsy-ml-systems-prod.feature_bank_v2.listing_feature_bank_most_recent`
),
merged as (
  select variantName, guid, query, listingId, price
  from semrel_results
  left join lfb using (listingId)
),
agg_results as (
  select variantName, guid, avg(price) as avg_price
  from merged 
  group by variantName, guid
)
select variantName, avg(avg_price) 
from agg_results
group by variantName