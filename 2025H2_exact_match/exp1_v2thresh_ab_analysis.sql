-- semrel metrics from experiment
with semrel_results as (
    SELECT variantName, guid, query, platform, userLanguage, listingId, classId
    FROM `etsy-data-warehouse-prod.search.sem_rel_hydrated_daily_requests_per_experiment` s
    left JOIN `etsy-data-warehouse-prod.search.sem_rel_query_listing_metrics_per_experiment` m
    USING (tableUUID, date)
    -- WHERE configFlag = "ranking/isearch.exact_match_unify_with_v2_loc"
    WHERE configFlag = "ranking/search.mmx.2025_q3.exact_match_unify_with_v2_si"
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




---- candidate set change
declare run_date date default "2025-07-31";

with semrel_results as (
    SELECT distinct variantName, mmxRequestUUID
    FROM `etsy-data-warehouse-prod.search.sem_rel_hydrated_daily_requests_per_experiment` s
    WHERE configFlag = "ranking/isearch.exact_match_unify_with_v2_loc"
    and date = run_date
),
rpc_data as (
  SELECT
    response.mmxRequestUUID,
    (SELECT COUNTIF(stage='POST_SEM_REL_FILTER') FROM UNNEST(OrganicRequestMetadata.candidateSources)) nPostSemrelSources,
    OrganicRequestMetadata.candidateSources
  FROM `etsy-searchinfra-gke-prod-2.thrift_mmx_listingsv2search_search.rpc_logs_*`
  WHERE request.OPTIONS.cacheBucketId LIKE "live%" 
  AND request.options.userCountry != 'US' -- filtering for domestic only
  AND request.options.csrOrganic 
  AND request.options.searchPlacement IN ('wsg', 'allsr', 'wmg') -- search, market
  AND request.query <> ''
  AND request.offset = 0
  AND DATE(queryTime) = run_date
  AND request.options.interleavingConfig IS NULL
  AND NOT EXISTS (
    SELECT * FROM UNNEST(request.context)
    WHERE key = "req_source" AND value = "bot"  -- remove bot requests
  )
),
blending as (
  SELECT distinct 
    mmxRequestUUID,
    array_length(cs.listingIds) n_candidates
  FROM rpc_data, UNNEST(candidateSources) cs
  WHERE cs.stage = IF(nPostSemrelSources>0, "POST_SEM_REL_FILTER", "POST_BORDA")
),
semrel_rpc as (
  select 
    variantName,
    mmxRequestUUID,
    n_candidates
  from semrel_results
  join blending using (mmxRequestUUID)
),
select variantName, avg(n_candidates)
from semrel_rpc
group by variantName


DECLARE start_date date DEFAULT '2025-07-31';
DECLARE end_date date DEFAULT '2025-08-02';
DECLARE sample_rate FLOAT64 DEFAULT 0.01; -- 0.01 will sample at 1% i.e. 1/100 rows (queries)
DECLARE my_experiment STRING default 'ranking/isearch.exact_match_unify_with_v2_loc';
WITH rpc_data AS (
  SELECT
    NULLIF(request.query, '') query,
    (SELECT NULLIF(query, '') FROM UNNEST(request.filter.query.translations) WHERE language = 'en') query_en, -- machine translated query if exists
    request.options.userLanguage,
    request.options.userCountry,
    request.options.browserId,
    CASE WHEN request.options.userCountry = 'US' THEN 'Domestic' ELSE 'International' END AS region,
    request.options.searchPlacement,
    response.count numResults,
    queryTime,
    DATE(queryTime) date,
    response.mmxRequestUUID mmx_request_uuid,
    (SELECT
        CASE
          WHEN value = 'web' THEN 'desktop'
          WHEN value = 'web_mobile' THEN 'mweb'
          WHEN value IN ('etsy_app_android', 'etsy_app_ios', 'etsy_app_other') THEN 'boe'
          ELSE value
        END
      FROM UNNEST(request.context) WHERE key = "req_source" ) platform,
    (SELECT COUNTIF(stage='POST_SEM_REL_FILTER') FROM UNNEST(OrganicRequestMetadata.candidateSources)) nPostSemrelSources,
    OrganicRequestMetadata.candidateSources
  FROM `etsy-searchinfra-gke-prod-2.thrift_mmx_listingsv2search_search.rpc_logs_*`
  WHERE request.OPTIONS.cacheBucketId LIKE "live%" 
    -- AND request.options.userCountry = 'US' -- filtering for domestic only
    AND request.options.csrOrganic 
    AND request.options.searchPlacement IN ('wsg', 'allsr', 'wmg') -- search, market
    AND request.query <> ''
    AND request.offset = 0
    AND DATE(queryTime) >= start_date AND DATE(queryTime) <= end_date 
    AND request.options.interleavingConfig IS NULL
    AND NOT EXISTS (
      SELECT * FROM UNNEST(request.context)
      WHERE key = "req_source" AND value = "bot"  -- remove bot requests
    )
    AND ABS(MOD(FARM_FINGERPRINT(response.mmxRequestUUID), 10000)) <= sample_rate * 10000 -- abs(mod(hash)) will return 0-9999 (10K) and only return 0.01*10k (100) out of 10K rows
),
rpc_results as (
  SELECT rpc_data.*, exp_data.variant_id FROM rpc_data 
  JOIN (
    SELECT bucketing_id, variant_id, bucketing_ts
    FROM `etsy-data-warehouse-prod.catapult_unified.bucketing_period`
    WHERE _date = end_date
    AND experiment_id = my_experiment
  ) exp_data ON rpc_data.browserId = exp_data.bucketing_id AND rpc_data.queryTime >= exp_data.bucketing_ts
),
blending AS (
  SELECT 
    variant_id,
    mmx_request_uuid,
    array_length(cs.listingIds) as n_candidates
  FROM rpc_results, UNNEST(candidateSources) cs
  WHERE cs.stage = IF(nPostSemrelSources>0, "POST_SEM_REL_FILTER", "POST_BORDA")
)
select variant_id, avg(n_candidates)
from blending 
group by variant_id
