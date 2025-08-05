WITH rpc AS (
    SELECT
        response.mmxRequestUUID,
        request.query AS query,
        request.context AS context,
        OrganicRequestMetadata.candidateSources AS candidateSources,
        response.semanticRelevanceModelInfo.modelSetName as sem_rel_modelset_name,
        response.semanticRelevanceScores AS semanticRelevanceScores,
        (SELECT COUNTIF(stage='POST_SEM_REL_FILTER')
         FROM UNNEST(OrganicRequestMetadata.candidateSources)) nPostSemrelSources
    FROM `etsy-searchinfra-gke-prod-2.thrift_mmx_listingsv2search_search.rpc_logs_*`
    WHERE 
        queryTime BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 MINUTE)  -- seconds
      AND CURRENT_TIMESTAMP() AND 
        request.options.cacheBucketId LIKE 'live%'
        AND request.query <> ''
        AND request.options.csrOrganic
        AND request.options.searchPlacement IN ('wsg', 'allsr')
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
)
    
    SELECT
        rpc.mmxRequestUUID,
        rpc.query,
        cs.stage,
        cs.source,
        listing_id,
        sem_rel_modelset_name,
        rank+1 AS rank,
        IF(semanticRelevanceScores IS NOT NULL AND ARRAY_LENGTH(semanticRelevanceScores) > rank,
           semanticRelevanceScores[OFFSET(rank)].relevanceScore, NULL) AS rel_score,
        IF(semanticRelevanceScores IS NOT NULL AND ARRAY_LENGTH(semanticRelevanceScores) > rank,
           semanticRelevanceScores[OFFSET(rank)].partialRelevanceScore, NULL) AS partial_rel_score,
        IF(semanticRelevanceScores IS NOT NULL AND ARRAY_LENGTH(semanticRelevanceScores) > rank,
           semanticRelevanceScores[OFFSET(rank)].irrelevanceScore, NULL) AS irrel_score
    FROM rpc,
    UNNEST(rpc.candidateSources) cs,
    UNNEST(cs.listingIds) AS listing_id WITH OFFSET rank
    WHERE cs.stage = 'POST_BORDA'
        AND cs.listingIds IS NOT NULL
        AND ARRAY_LENGTH(cs.listingIds) > 0
        AND IF(semanticRelevanceScores IS NOT NULL AND ARRAY_LENGTH(semanticRelevanceScores) > rank,
           semanticRelevanceScores[OFFSET(rank)].relevanceScore, NULL) > 0
        and query = "wizard font embroidery"
        and listing_id in (1739358162, 1785655325)
    LIMIT 10000