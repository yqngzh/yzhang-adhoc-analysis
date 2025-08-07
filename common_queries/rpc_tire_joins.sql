--- find OrganicRequestMetadata for searchwithads TIRE test
SELECT
  a.OrganicRequestMetadata.candidateSources,
  c.tireRequestContext.variant,
FROM `etsy-searchinfra-gke-dev.thrift_mmx_listingsv2search_search.rpc_logs*` a
JOIN `etsy-searchinfra-gke-prod-2.thrift_tire_searchwithads_rpc_logs.rpc_logs_*` c
ON a.request.options.cacheBucketId LIKE "replay-test/%/y5bJzPoWGz9z8ehdIf1w/%|test1|live|web"
AND c.request.requestParams.organicRequest.options.cacheBucketId = "y5bJzPoWGz9z8ehdIf1w"
WHERE DATE(a.queryTime) = "2025-08-04" AND DATE(c.queryTime) = "2025-08-04"
AND EXISTS (
    SELECT 1 
    FROM UNNEST(a.OrganicRequestMetadata.candidateSources) AS candidateSource
    WHERE candidateSource.stage IS NOT NULL
)
GROUP BY
  a.OrganicRequestMetadata.candidateSources,
  c.tireRequestContext.variant



--- find both ads and organic listings 
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
