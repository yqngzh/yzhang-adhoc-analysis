create or replace table `etsy-search-ml-dev.search.yzhang_tire_BdtHg63mrNC5aA4xloUk` as (
  with control_requests as (
    select
      "control" as variantName,
      response.mmxRequestUUID,
      COALESCE((SELECT NULLIF(query, '') FROM UNNEST(request.filter.query.translations) WHERE language = 'en'), NULLIF(request.query, '')) query,
      OrganicRequestMetadata.candidateSources
    FROM `etsy-searchinfra-gke-dev.thrift_mmx_listingsv2search_search.rpc_logs*`
    WHERE request.options.cacheBucketId LIKE "replay-test/%/BdtHg63mrNC5aA4xloUk/%|control|live|web"
    and DATE(queryTime) = "2025-07-25"
    and EXISTS (
      SELECT 1 
      FROM UNNEST(OrganicRequestMetadata.candidateSources) AS candidateSource
      WHERE candidateSource.stage is not null
    )
  ),
  test_requests as (
    select
      "test1" as variantName,
      response.mmxRequestUUID,
      COALESCE((SELECT NULLIF(query, '') FROM UNNEST(request.filter.query.translations) WHERE language = 'en'), NULLIF(request.query, '')) query,
      OrganicRequestMetadata.candidateSources
    FROM `etsy-searchinfra-gke-dev.thrift_mmx_listingsv2search_search.rpc_logs*`
    WHERE request.options.cacheBucketId LIKE "replay-test/%/BdtHg63mrNC5aA4xloUk/%|test1|live|web"
    and DATE(queryTime) = "2025-07-25"
    and EXISTS (
      SELECT 1 
      FROM UNNEST(OrganicRequestMetadata.candidateSources) AS candidateSource
      WHERE candidateSource.stage is not null
    )
  )
  select 
)