---- Get a day of search requests before semrel filtering
-- date: 2025-07-19

declare sampleDate DATE default '2025-07-09';

create or replace table `etsy-search-ml-dev.search.yzhang_emcf_rpc_07-19` as (
    with allRequests as (
        SELECT
            RequestIdentifiers.etsyRootUUID as rootUUID,
            RequestIdentifiers.etsyRequestUUID as etsyUUID,
            response.mmxRequestUUID as mmxRequestUUID,
            COALESCE((SELECT NULLIF(query, '') FROM UNNEST(request.filter.query.translations) WHERE language = 'en'), NULLIF(request.query, '')) query,
            request.options.userLanguage userLanguage,
            request.options.userCountry userCountry,
            request.options.searchPlacement searchPlacement,
            DATE(queryTime) queryDate,
            (SELECT
                CASE
                    WHEN value = 'web' THEN 'web'
                    WHEN value = 'web_mobile' THEN 'mweb'
                    WHEN value IN ('etsy_app_android', 'etsy_app_ios', 'etsy_app_other') THEN 'boe'
                    ELSE value
                END
                FROM unnest(request.context)
                WHERE key = "req_source"
            ) as requestSource,
            OrganicRequestMetadata.candidateSources candidateSources,
            (SELECT COUNTIF(stage='POST_FILTER') FROM UNNEST(OrganicRequestMetadata.candidateSources)) nPostFilterSources,
            (SELECT COUNTIF(stage='POST_BORDA') FROM UNNEST(OrganicRequestMetadata.candidateSources)) nPostBordaSources,
            (SELECT COUNTIF(stage='POST_SEM_REL_FILTER') FROM UNNEST(OrganicRequestMetadata.candidateSources)) nPostSemrelSources,
        FROM `etsy-searchinfra-gke-prod-2.thrift_mmx_listingsv2search_search.rpc_logs_*`
        WHERE request.OPTIONS.cacheBucketId LIKE "live%"
        AND request.options.csrOrganic
        AND request.options.searchPlacement IN ('wsg', 'allsr', 'wmg')
        AND request.query != ''
        AND DATE(queryTime) = sampleDate
        AND request.options.interleavingConfig IS NULL
        AND NOT EXISTS (
            SELECT * FROM UNNEST(request.context)
            WHERE key = "req_source" AND value = "bot"
        )
    ),
    validRequests AS (
        SELECT *
        FROM allRequests
        WHERE nPostFilterSources>=1
        AND (nPostBordaSources>=1 OR nPostSemrelSources>=1)
    ),
    reqsSample AS (
        SELECT * FROM validRequests
        WHERE RAND() < 0.01
    ),

)
