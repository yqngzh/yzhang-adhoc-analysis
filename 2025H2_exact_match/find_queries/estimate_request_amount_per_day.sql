with request_level_impression as (
    select distinct
        query,
        visit_id,
        mmx_request_uuid,
        count(*) as per_request_impression
    from `etsy-data-warehouse-prod.rollups.search_impressions`
    where _date = date("2025-09-06")
    and query is not null and query != ""
    group by query, visit_id, mmx_request_uuid
),
query_level_impression as (
    select query, sum(per_request_impression) as per_query_impression
    from request_level_impression
    group by query
),
top_impressed_queries as (
    select query
    from query_level_impression
    order by per_query_impression desc
    limit 1000
),
rpc AS (
    SELECT distinct
        response.mmxRequestUUID,
        request.query AS query,
    FROM `etsy-searchinfra-gke-prod-2.thrift_mmx_listingsv2search_search.rpc_logs_*`
    WHERE date(queryTime) = date("2025-09-06")
    AND request.options.cacheBucketId LIKE 'live%'
    AND request.query <> ''
    AND request.options.csrOrganic
    AND request.options.searchPlacement IN ('wsg', "wmg", "allsr")
    AND request.offset = 0
    AND request.options.interleavingConfig IS NULL
    AND OrganicRequestMetadata IS NOT NULL
    AND EXISTS (
        SELECT 1 FROM UNNEST(OrganicRequestMetadata.candidateSources) cs
        WHERE cs.stage IN ('POST_FILTER', 'POST_SEM_REL_FILTER', 'POST_BORDA', 'RANKING', 'MO_LASTPASS')
            AND cs.listingIds IS NOT NULL
            AND ARRAY_LENGTH(cs.listingIds) > 0
    )
    AND NOT EXISTS (
        SELECT 1 FROM UNNEST(request.context)
        WHERE key = 'req_source' AND value = 'bot'
    )
    AND response.mmxRequestUUID IS NOT NULL
),
sampled_requests as (
    select *
    from rpc
    where query in (
        select query from top_impressed_queries
    )
)
select count(*) from sampled_requests

-- 09-06: 766308
-- 09-05: 