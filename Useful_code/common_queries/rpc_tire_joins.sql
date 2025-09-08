------  In TIRE, request source  ------
SELECT request, queryTime 
FROM `etsy-searchinfra-gke-prod-2.thrift_mmx_recsys_searchwithads_rpc_logs.rpc_logs*` 
WHERE queryTime >= "2025-07-23 17:00:00+00:00"
AND queryTime < "2025-07-23 18:00:00+00:00"
AND request.requestParams.organicRequest is not null 
AND request.requestParams.organicRequest.options.cacheBucketId = "live|web"
AND request.requestParams.organicRequest.options.queryType = "ponycorn_seller_quality"
AND request.requestParams.organicRequest.options.csrOrganic=true
AND request.requestParams.organicRequest.offset = 0
AND NOT EXISTS (SELECT * FROM UNNEST(request.requestParams.context) WHERE key = "req_source" AND value = "bot") 
-- SO
AND request.requestParams.organicRequest.options.searchPlacement in unnest(["wsg", "wmg"])
AND request.requestParams.organicRequest.options.userCountry = "US"
AND request.requestParams.organicRequest.options.personalizationOptions.userId = 0
-- SI
-- AND request.requestParams.organicRequest.options.searchPlacement in unnest(["allsr", "wsg", "wmg"])
-- AND request.requestParams.organicRequest.options.userCountry = "US"
-- AND request.requestParams.organicRequest.options.personalizationOptions.userId != 0
AND runtime.sampleVal < 0.17142857142857144
ORDER BY runtime.sampleVal
LIMIT 13750
-- N = rps * duration + (rampup_seconds / 2 * rps)



------  Find OrganicRequestMetadata for searchwithads TIRE test  ------  
WITH tmp AS (
  SELECT
    a.response.mmxRequestUUID,
    a.OrganicRequestMetadata.candidateSources,
    c.tireRequestContext.variant
  FROM `etsy-searchinfra-gke-dev.thrift_mmx_listingsv2search_search.rpc_logs*` a
  JOIN `etsy-searchinfra-gke-dev.thrift_tire_searchwithads_rpc_logs.rpc_logs_*` c
  ON a.response.mmxRequestUUID = c.response.preserved.organicResults.mmxRequestUUID
  AND c.tireRequestContext.tireTestv2Id = "y5bJzPoWGz9z8ehdIf1w"
  WHERE DATE(a.queryTime) = "2025-08-04" AND DATE(c.queryTime) = "2025-08-04"
  AND EXISTS (
    SELECT 1
    FROM UNNEST(a.OrganicRequestMetadata.candidateSources) AS cs
    WHERE cs.stage IS NOT NULL
  )
)
SELECT
  variant,
  COUNT(*) AS rows_per_variant
FROM tmp
GROUP BY variant

-- can still use listingsv2search
SELECT
    response.mmxRequestUUID,
    OrganicRequestMetadata.candidateSources,
    REGEXP_EXTRACT(
        request.options.cacheBucketId,
        r"\|([^|]+)\|live\|web$"
    ) AS variant
FROM `etsy-searchinfra-gke-dev.thrift_mmx_listingsv2search_search.rpc_logs*` a
WHERE request.options.cacheBucketId LIKE "replay-test/%/TIREID/%|%|live|web"
AND DATE(a.queryTime) = "2025-08-04"
AND EXISTS (
    SELECT 1
    FROM UNNEST(a.OrganicRequestMetadata.candidateSources) AS cs
    WHERE cs.stage IS NOT NULL
)



------  Get blended page 1 search + ads listings POST- JOINS  ------  
SELECT
    tireRequestContext.tireRequestUUID,
    r.id.listingId,
    r.candidateSource
FROM `etsy-searchinfra-gke-dev.thrift_tire_searchwithads_rpc_logs.rpc_logs_*`,
    UNNEST(response.dynamicResults.results) AS r
WHERE tireRequestContext.tireTestv2Id = "TIREID"
AND DATE(queryTime) = "2025-08-04"

-- separate ads and organic
SELECT
  tireRequestContext.tireRequestUUID,
  ARRAY_AGG(
    CASE WHEN r.candidateSource = "mmxslv2-PonycornSellerQuality" THEN r.id.listingId END IGNORE NULLS
  ) AS organic_listing_ids,
  ARRAY_AGG(
    CASE WHEN r.candidateSource = "mmxslv2-Prolist" THEN r.id.listingId END IGNORE NULLS
  ) AS ads_listing_ids
FROM `etsy-searchinfra-gke-dev.thrift_tire_searchwithads_rpc_logs.rpc_logs_*`,
  UNNEST(response.dynamicResults.results) AS r
WHERE tireRequestContext.tireTestv2Id = "eZUe8cpbYhN4OmxaOGdm"
AND DATE(queryTime) = "2025-08-06"
GROUP BY tireRequestContext.tireRequestUUID



------  Get page 1 organic and ads listings separately PRE- JOINS  ------  
WITH tire_data AS (
  SELECT
        response.preserved.organicResults.mmxRequestUUID,
        tireRequestContext.variant AS variant,
        request.requestParams.organicRequest.query AS query,
        request.requestParams.organicRequest.limit AS organic_limit,
        request.requestParams.organicRequest.offset AS organic_offset,
        request.requestParams.adsRequest.limit AS ads_limit,
        request.requestParams.adsRequest.offset AS ads_offset,
        response.preserved.organicResults.listingIds AS organic_listing_ids,
        response.preserved.adsResults.listingIds AS ads_listing_ids
  FROM `etsy-searchinfra-gke-dev.thrift_tire_searchwithads_rpc_logs.rpc_logs_*`
  WHERE tireRequestContext.tireTestv2Id = "TIREID"
  AND DATE(queryTime) = "2025-08-04" 
),
organic_listings_control AS (
  SELECT mmxRequestUUID, variant, query, listing_id
  FROM tire_data, UNNEST(organic_listing_ids) AS listing_id
  WHERE variant = "control"
  AND ARRAY_LENGTH(organic_listing_ids) > 0
  AND organic_offset = 0 
),
organic_listings_test AS (
  SELECT mmxRequestUUID, variant, query, listing_id
  FROM tire_data, UNNEST(organic_listing_ids) AS listing_id
  WHERE variant = "test1"
  AND ARRAY_LENGTH(organic_listing_ids) > 0
  AND organic_offset = 0 
)
