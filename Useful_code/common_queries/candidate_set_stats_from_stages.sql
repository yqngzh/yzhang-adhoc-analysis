WITH rpc AS (
    SELECT
        response.mmxRequestUUID,
        request.query AS query,
        date(queryTime) as queryDate,
        OrganicRequestMetadata.candidateSources AS candidateSources,
        response.semanticRelevanceModelInfo.modelSetName as sem_rel_modelset_name,
        response.semanticRelevanceScores AS semanticRelevanceScores
    FROM `etsy-searchinfra-gke-prod-2.thrift_mmx_listingsv2search_search.rpc_logs_*`
    WHERE queryTime BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY) AND CURRENT_TIMESTAMP()
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

stage_counts AS (
    SELECT
        rpc.mmxRequestUUID,
        rpc.query,
        rpc.queryDate,
        rpc.sem_rel_modelset_name,
        cs.stage,
        ARRAY_LENGTH(cs.listingIds) AS candidate_count,
    FROM rpc, UNNEST(rpc.candidateSources) cs
    WHERE cs.stage IN ('POST_FILTER', 'POST_SEM_REL_FILTER', 'POST_BORDA', 'RANKING', 'MO_LASTPASS')
    AND cs.listingIds IS NOT NULL
    AND ARRAY_LENGTH(cs.listingIds) > 0
),

semrel_filtered_count as (
    select 
        mmxRequestUUID,
        query,
        queryDate,
        SUM(CASE WHEN stage = 'POST_FILTER' THEN candidate_count END) 
            - SUM(CASE WHEN stage = 'POST_SEM_REL_FILTER' THEN candidate_count END) as n_filtered
    from stage_counts
    group by mmxRequestUUID, query, queryDate
)

select max(n_filtered) from semrel_filtered_count

