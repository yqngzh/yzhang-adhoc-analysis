---- In TIRE, request source
SELECT request, queryTime
FROM `etsy-searchinfra-gke-prod-2.thrift_mmx_listingsv2search_search.rpc_logs_*`
-- Sign in
WHERE request.options.searchPlacement in unnest(["allsr", "wsg"])
AND request.options.personalizationOptions.userId != 0
-- Sign out
-- request.options.searchPlacement in unnest(["wsg"])
-- AND request.options.personalizationOptions.userId = 0
AND request.options.userCountry = "US"
AND queryTime >= "2024-11-01 17:00:00+00:00"
AND queryTime < "2024-11-01 18:00:00+00:00"
AND request.OPTIONS.cacheBucketId = "live|web"
AND request.options.queryType = "ponycorn_seller_quality"
AND request.offset = 0  -- get 1st pages
AND request.options.solrRetrievalOnly = FALSE
AND request.sortBy = 'relevance'
AND request.sortOrder = 'desc'
AND request.options.mmxBehavior.matching IS NOT NULL
AND request.options.csrOrganic = TRUE
AND NOT EXISTS (SELECT * FROM UNNEST(request.context) WHERE KEY = "req_source" AND value = "bot")
AND request.options.solrRetrievalOnly = FALSE
AND OrganicRequestMetadata.candidateSources IS NOT NULL
AND runtime.sampleVal < 0.17142857142857144
ORDER BY runtime.sampleVal
LIMIT 33750



----  get OrganicRequestMetadata from TIRE tests
create or replace table `etsy-search-ml-dev.search.yzhang_em_tire_bKVAhAdqceVYmjXbrFX0` as (
  with control_requests as (
    select
      response.mmxRequestUUID,
      COALESCE((SELECT NULLIF(query, '') FROM UNNEST(request.filter.query.translations) WHERE language = 'en'), NULLIF(request.query, '')) query,
      OrganicRequestMetadata.candidateSources,
      (SELECT COUNTIF(stage='POST_SEM_REL_FILTER') FROM UNNEST(OrganicRequestMetadata.candidateSources)) nPostSemrelSources
    FROM `etsy-searchinfra-gke-dev.thrift_mmx_listingsv2search_search.rpc_logs*`
    WHERE request.options.cacheBucketId LIKE "replay-test/%/bKVAhAdqceVYmjXbrFX0/%|control|live|web"
    and DATE(queryTime) = "2025-08-05"
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
    WHERE request.options.cacheBucketId LIKE "replay-test/%/bKVAhAdqceVYmjXbrFX0/%|test1|live|web"
    and DATE(queryTime) = "2025-08-05"
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
          COALESCE(NULLIF(verticaListings_title, ''), NULLIF(verticaListingTranslations_machineTranslatedEnglishTitle, '')),
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
  where query is not null and query != ""
)

-- or to do both variants in one call using regex
WITH calls AS (
  SELECT
    response.mmxRequestUUID,
    OrganicRequestMetadata.candidateSources,
    -- grab the bit between the two |'s right before live|web
    REGEXP_EXTRACT(
      request.options.cacheBucketId,
      r"\|([^|]+)\|live\|web$"
    ) AS variant
  FROM `etsy-searchinfra-gke-dev.thrift_mmx_listingsv2search_search.rpc_logs*` a
  WHERE request.options.cacheBucketId LIKE "replay-test/%/y5bJzPoWGz9z8ehdIf1w/%|%|live|web"
  AND DATE(a.queryTime) = "2025-08-04"
  AND EXISTS (
    SELECT 1
    FROM UNNEST(a.OrganicRequestMetadata.candidateSources) AS cs
    WHERE cs.stage IS NOT NULL
  )
)
SELECT variant, COUNT(*) AS rows_per_variant
FROM calls
GROUP BY variant;
