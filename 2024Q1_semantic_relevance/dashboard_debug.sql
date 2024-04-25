CREATE OR REPLACE TABLE FUNCTION `etsy-sr-etl-prod.yzhang.sem_rel_sample_query_after`(
    sampleDate DATE,
    minResults INT64,
    samplingRate FLOAT64,
    samplePlatform STRING, 
    listingPerPage INT64
)
AS
WITH allRequests AS (
    SELECT
        (SELECT COUNTIF(stage='MO_LASTPASS') FROM UNNEST(OrganicRequestMetadata.candidateSources)) nLastpassSources,
        (SELECT COUNTIF(stage='POST_FILTER') FROM UNNEST(OrganicRequestMetadata.candidateSources)) nPostFilterSources,
        (SELECT COUNTIF(stage='POST_BORDA') FROM UNNEST(OrganicRequestMetadata.candidateSources)) nPostBordaSources,
        request.query AS query,
        request.options.userLanguage userLanguage,
        request.options.locale userLocale,
        response.count numResults,
        request.filter.maturity as matureFilter,
        DATE(queryTime) date,
        RequestIdentifiers.etsyRequestUUID as etsyUUID,
        response.mmxRequestUUID as mmxRequestUUID,
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
        OrganicRequestMetadata.candidateSources candidateSources
    FROM `etsy-searchinfra-gke-prod-2.thrift_mmx_listingsv2search_search.rpc_logs_*`
    WHERE request.OPTIONS.cacheBucketId LIKE "live%"
    AND request.options.csrOrganic
    AND request.query <> ''
    AND request.offset = 0
    AND DATE(queryTime) = sampleDate
    -- AND request.filter.maturity = 'safe_only'
    AND request.options.interleavingConfig IS NULL
    AND response.count >= minResults
), 
validRequests AS (
    SELECT * FROM allRequests 
    WHERE requestSource = samplePlatform
    AND nLastPassSources=1
    AND nPostFilterSources>=1
    AND nPostBordaSources>=1
    -- AND NOT ENDS_WITH(query, ' -mature')
),
reqsSample AS (
    SELECT * FROM validRequests
    WHERE RAND() < samplingRate
),
beacon AS (
    SELECT DISTINCT
        beacon.guid,
        visit_id,
        (SELECT value FROM UNNEST(beacon.properties.key_value) WHERE key = 'mmx_request_uuid') AS mmxRequestUUID,
    FROM `etsy-visit-pipe-prod.canonical.visit_id_beacons`
    WHERE DATE(_PARTITIONTIME) = sampleDate
    AND beacon.event_name = 'search'
),
lfb AS (
    SELECT 
        key listingId, 
        verticaListings_taxonomyPath listingTaxo,
        NULLIF(verticaListings_title, "") listingTitle,
        verticaListings_description  listingDescription,
        verticaListings_tags listingTags,
    FROM `etsy-ml-systems-prod.feature_bank_v2.listing_feature_bank_most_recent`
),
qis AS (
    SELECT 
        query_raw query,
        CASE 
            WHEN class_id = 0 THEN 'broad' 
            WHEN class_id = 1 THEN 'direct_unspecified'
            WHEN class_id = 2 THEN 'direct_specified' 
        END AS qisClass
    FROM `etsy-search-ml-prod.mission_understanding.qis_scores`
),
qlm AS (
    SELECT query_raw query, _date date, bin 
    FROM `etsy-batchjobs-prod.snapshots.query_level_metrics_raw`
    WHERE _date BETWEEN DATE_SUB(sampleDate, INTERVAL 1 DAY) AND DATE_ADD(sampleDate, INTERVAL 1 DAY)
),
qfb AS (
  SELECT key query, queryTaxoClassification_taxoPath queryTaxo
  FROM `etsy-ml-systems-prod.feature_bank_v2.query_feature_bank_most_recent`
),
queryHydratedRequests AS (
    SELECT
        etsyUUID,
        mmxRequestUUID,
        guid,
        visit_id,
        query,
        REGEXP_CONTAINS(query, '(\?i)\\bgift|\\bfor (\\bhim|\\bher|\\bmom|\\bdad|\\bmother|\\bfather|\\bdaughter|\\bson|\\bwife|\\bhusband|\\bpartner|\\baunt|\\buncle|\\bniece|\\bnephew|\\bfiance|\\bcousin|\\bin law|\\bboyfriend|\\bgirlfriend|\\bgrand|\\bfriend|\\bbest friend)') isGift,
        IFNULL(qlm.bin, 'novel') queryBin,
        IFNULL(qis.qisClass, 'missing') qisClass,
        queryTaxo,
        requestSource platform,
        userLanguage,
        numResults,
        matureFilter,
        reqsSample.date,
        ARRAY_CONCAT(
            ARRAY(
                SELECT STRUCT(listing_id AS listingId, idx AS rankingRank, CAST(NULL AS INT64) AS retrievalRank, CAST(NULL AS STRING) AS retrievalSrc, CAST(NULL AS INT64) AS bordaRank, 1 AS pageNum)
                FROM UNNEST(candidateSources) cs,
                    UNNEST(cs.listingIds) AS listing_id WITH OFFSET idx
                WHERE cs.stage = "MO_LASTPASS"
                AND idx < listingPerPage
            ),
            ARRAY(
                SELECT STRUCT(listing_id AS listingId, idx AS rankingRank, CAST(NULL AS INT64) AS retrievalRank, CAST(NULL AS STRING) AS retrievalSrc, CAST(NULL AS INT64) AS bordaRank, 2 AS pageNum)
                FROM UNNEST(candidateSources) cs,
                    UNNEST(cs.listingIds) AS listing_id WITH OFFSET idx
                WHERE cs.stage = "MO_LASTPASS"
                AND idx >= listingPerPage AND idx < listingPerPage * 2
                ORDER BY RAND()
                LIMIT 10
            ),
            ARRAY(
                SELECT STRUCT(listing_id AS listingId, idx AS rankingRank, CAST(NULL AS INT64) AS retrievalRank, CAST(NULL AS STRING) AS retrievalSrc, CAST(NULL AS INT64) AS bordaRank, 3 AS pageNum)
                FROM UNNEST(candidateSources) cs,
                    UNNEST(cs.listingIds) AS listing_id WITH OFFSET idx
                WHERE cs.stage = "MO_LASTPASS"
                AND idx >= listingPerPage * 2 AND idx < listingPerPage * 3
                ORDER BY RAND()
                LIMIT 10
            ),
            ARRAY(
                SELECT STRUCT(listing_id AS listingId, CAST(NULL AS INT64) AS rankingRank, idx AS retrievalRank, cs.source AS retrievalSrc, CAST(NULL AS INT64) AS bordaRank, CAST(NULL AS INT64) AS pageNum)
                FROM UNNEST(candidateSources) cs,
                    UNNEST(cs.listingIds) AS listing_id WITH OFFSET idx
                WHERE cs.stage = "POST_FILTER"
                ORDER BY RAND()
                LIMIT 10
            ),
            ARRAY(
                SELECT STRUCT(listing_id AS listingId, CAST(NULL AS INT64) AS rankingRank, CAST(NULL AS INT64) AS retrievalRank, CAST(NULL AS STRING) AS retrievalSrc, idx AS bordaRank, CAST(NULL AS INT64) AS pageNum)
                FROM UNNEST(candidateSources) cs,
                    UNNEST(cs.listingIds) AS listing_id WITH OFFSET idx
                WHERE cs.stage = "POST_BORDA"
                ORDER BY RAND()
                LIMIT 10
            )
        ) listingSamples
    FROM reqsSample
    LEFT JOIN qlm USING (query, date)
    LEFT JOIN qis USING (query)
    LEFT JOIN qfb USING (query)
    LEFT JOIN beacon USING (mmxRequestUUID)
), 
flatQueryHydratedRequests AS (
    SELECT * EXCEPT (listingSamples)
    FROM queryHydratedRequests,
        UNNEST(queryHydratedRequests.listingSamples) listingSample
    WHERE guid IS NOT NULL
),
outputTable AS (
    SELECT 
        flatQueryHydratedRequests.*,
        lfb.listingTitle,
        lfb.listingDescription,
        lfb.listingTaxo,
        lfb.listingTags,
    FROM flatQueryHydratedRequests
    LEFT JOIN lfb USING(listingId)
    WHERE listingTitle IS NOT NULL
)
SELECT
    GENERATE_UUID() AS tableUUID,
    outputTable.*
FROM outputTable;




CREATE OR REPLACE TABLE FUNCTION `etsy-sr-etl-prod.yzhang.sem_rel_sample_query_before`(
    sampleDate DATE,
    minResults INT64,
    samplingRate FLOAT64,
    samplePlatform STRING, 
    listingPerPage INT64
)
AS
WITH allRequests AS (
    SELECT
        (SELECT COUNTIF(stage='MO_LASTPASS') FROM UNNEST(OrganicRequestMetadata.candidateSources)) nLastpassSources,
        (SELECT COUNTIF(stage='POST_FILTER') FROM UNNEST(OrganicRequestMetadata.candidateSources)) nPostFilterSources,
        (SELECT COUNTIF(stage='POST_BORDA') FROM UNNEST(OrganicRequestMetadata.candidateSources)) nPostBordaSources,
        request.query AS query,
        request.options.userLanguage userLanguage,
        request.options.locale userLocale,
        response.count numResults,
        DATE(queryTime) date,
        RequestIdentifiers.etsyRequestUUID as etsyUUID,
        response.mmxRequestUUID as mmxRequestUUID,
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
        OrganicRequestMetadata.candidateSources candidateSources
    FROM `etsy-searchinfra-gke-prod-2.thrift_mmx_listingsv2search_search.rpc_logs_*`
    WHERE request.OPTIONS.cacheBucketId LIKE "live%"
    AND request.options.csrOrganic
    AND request.query <> ''
    AND request.offset = 0
    AND DATE(queryTime) = sampleDate
    AND request.filter.maturity = 'safe_only'
    AND request.options.interleavingConfig IS NULL
    AND response.count >= minResults
), 
validRequests AS (
    SELECT * FROM allRequests 
    WHERE requestSource = samplePlatform
    AND nLastPassSources=1
    AND nPostFilterSources>=1
    AND nPostBordaSources>=1
    AND NOT ENDS_WITH(query, ' -mature')
),
reqsSample AS (
    SELECT * FROM validRequests
    WHERE RAND() < samplingRate
),
beacon AS (
    SELECT DISTINCT
        beacon.guid,
        (SELECT value FROM UNNEST(beacon.properties.key_value) WHERE key = 'mmx_request_uuid') AS mmxRequestUUID,
    FROM `etsy-visit-pipe-prod.canonical.visit_id_beacons`
    WHERE DATE(_PARTITIONTIME) = sampleDate
    AND beacon.event_name = 'search'
),
lfb AS (
    SELECT 
        key listingId, 
        verticaListings_taxonomyPath listingTaxo,
        NULLIF(verticaListings_title, "") listingTitle,
        verticaListings_description  listingDescription,
        verticaListings_tags listingTags,
    FROM `etsy-ml-systems-prod.feature_bank_v2.listing_feature_bank_most_recent`
),
qis AS (
    SELECT 
        query_raw query,
        CASE 
            WHEN class_id = 0 THEN 'broad' 
            WHEN class_id = 1 THEN 'direct_unspecified'
            WHEN class_id = 2 THEN 'direct_specified' 
        END AS qisClass
    FROM `etsy-search-ml-prod.mission_understanding.qis_scores`
),
qlm AS (
    SELECT query_raw query, _date date, bin 
    FROM `etsy-batchjobs-prod.snapshots.query_level_metrics_raw`
    WHERE _date BETWEEN DATE_SUB(sampleDate, INTERVAL 1 DAY) AND DATE_ADD(sampleDate, INTERVAL 1 DAY)
),
qfb AS (
  SELECT key query, queryTaxoClassification_taxoPath queryTaxo
  FROM `etsy-ml-systems-prod.feature_bank_v2.query_feature_bank_most_recent`
),
queryHydratedRequests AS (
    SELECT
        etsyUUID,
        mmxRequestUUID,
        guid,
        query,
        REGEXP_CONTAINS(query, '(\?i)\\bgift|\\bfor (\\bhim|\\bher|\\bmom|\\bdad|\\bmother|\\bfather|\\bdaughter|\\bson|\\bwife|\\bhusband|\\bpartner|\\baunt|\\buncle|\\bniece|\\bnephew|\\bfiance|\\bcousin|\\bin law|\\bboyfriend|\\bgirlfriend|\\bgrand|\\bfriend|\\bbest friend)') isGift,
        IFNULL(qlm.bin, 'novel') queryBin,
        IFNULL(qis.qisClass, 'missing') qisClass,
        queryTaxo,
        requestSource platform,
        userLanguage,
        numResults,
        reqsSample.date,
        ARRAY_CONCAT(
            ARRAY(
                SELECT STRUCT(listing_id AS listingId, idx AS rankingRank, CAST(NULL AS INT64) AS retrievalRank, CAST(NULL AS STRING) AS retrievalSrc, CAST(NULL AS INT64) AS bordaRank, 1 AS pageNum)
                FROM UNNEST(candidateSources) cs,
                    UNNEST(cs.listingIds) AS listing_id WITH OFFSET idx
                WHERE cs.stage = "MO_LASTPASS"
                AND idx < listingPerPage
            ),
            ARRAY(
                SELECT STRUCT(listing_id AS listingId, idx AS rankingRank, CAST(NULL AS INT64) AS retrievalRank, CAST(NULL AS STRING) AS retrievalSrc, CAST(NULL AS INT64) AS bordaRank, 2 AS pageNum)
                FROM UNNEST(candidateSources) cs,
                    UNNEST(cs.listingIds) AS listing_id WITH OFFSET idx
                WHERE cs.stage = "MO_LASTPASS"
                AND idx >= listingPerPage AND idx < listingPerPage * 2
                ORDER BY RAND()
                LIMIT 10
            ),
            ARRAY(
                SELECT STRUCT(listing_id AS listingId, idx AS rankingRank, CAST(NULL AS INT64) AS retrievalRank, CAST(NULL AS STRING) AS retrievalSrc, CAST(NULL AS INT64) AS bordaRank, 3 AS pageNum)
                FROM UNNEST(candidateSources) cs,
                    UNNEST(cs.listingIds) AS listing_id WITH OFFSET idx
                WHERE cs.stage = "MO_LASTPASS"
                AND idx >= listingPerPage * 2 AND idx < listingPerPage * 3
                ORDER BY RAND()
                LIMIT 10
            ),
            ARRAY(
                SELECT STRUCT(listing_id AS listingId, CAST(NULL AS INT64) AS rankingRank, idx AS retrievalRank, cs.source AS retrievalSrc, CAST(NULL AS INT64) AS bordaRank, CAST(NULL AS INT64) AS pageNum)
                FROM UNNEST(candidateSources) cs,
                    UNNEST(cs.listingIds) AS listing_id WITH OFFSET idx
                WHERE cs.stage = "POST_FILTER"
                ORDER BY RAND()
                LIMIT 10
            ),
            ARRAY(
                SELECT STRUCT(listing_id AS listingId, CAST(NULL AS INT64) AS rankingRank, CAST(NULL AS INT64) AS retrievalRank, CAST(NULL AS STRING) AS retrievalSrc, idx AS bordaRank, CAST(NULL AS INT64) AS pageNum)
                FROM UNNEST(candidateSources) cs,
                    UNNEST(cs.listingIds) AS listing_id WITH OFFSET idx
                WHERE cs.stage = "POST_BORDA"
                ORDER BY RAND()
                LIMIT 10
            )
        ) listingSamples
    FROM reqsSample
    LEFT JOIN qlm USING (query, date)
    LEFT JOIN qis USING (query)
    LEFT JOIN qfb USING (query)
    LEFT JOIN beacon USING (mmxRequestUUID)
), 
flatQueryHydratedRequests AS (
    SELECT * EXCEPT (listingSamples)
    FROM queryHydratedRequests,
        UNNEST(queryHydratedRequests.listingSamples) listingSample
    WHERE guid IS NOT NULL
)
SELECT 
    flatQueryHydratedRequests.*,
    lfb.listingTitle,
    lfb.listingDescription,
    lfb.listingTaxo,
    lfb.listingTags,
FROM flatQueryHydratedRequests
LEFT JOIN lfb USING(listingId)
WHERE listingTitle IS NOT NULL;



CREATE OR REPLACE TABLE FUNCTION `etsy-sr-etl-prod.yzhang.sem_rel_sample_query_fix1`(
    sampleDate DATE,
    minResults INT64,
    samplingRate FLOAT64,
    samplePlatform STRING, 
    listingPerPage INT64
)
AS
WITH allRequests AS (
    SELECT
        (SELECT COUNTIF(stage='MO_LASTPASS') FROM UNNEST(OrganicRequestMetadata.candidateSources)) nLastpassSources,
        (SELECT COUNTIF(stage='POST_FILTER') FROM UNNEST(OrganicRequestMetadata.candidateSources)) nPostFilterSources,
        (SELECT COUNTIF(stage='POST_BORDA') FROM UNNEST(OrganicRequestMetadata.candidateSources)) nPostBordaSources,
        request.query AS query,
        request.options.userLanguage userLanguage,
        request.options.locale userLocale,
        response.count numResults,
        request.filter.maturity as matureFilter,
        DATE(queryTime) date,
        RequestIdentifiers.etsyRequestUUID as etsyUUID,
        response.mmxRequestUUID as mmxRequestUUID,
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
        OrganicRequestMetadata.candidateSources candidateSources
    FROM `etsy-searchinfra-gke-prod-2.thrift_mmx_listingsv2search_search.rpc_logs_*`
    WHERE request.OPTIONS.cacheBucketId LIKE "live%"
    AND request.options.csrOrganic
    AND request.query <> ''
    AND request.offset = 0
    AND DATE(queryTime) = sampleDate
    -- AND request.filter.maturity = 'safe_only'
    AND request.options.interleavingConfig IS NULL
    AND response.count >= minResults
), 
validRequests AS (
    SELECT * FROM allRequests 
    WHERE requestSource = samplePlatform
    AND nLastPassSources=1
    AND nPostFilterSources>=1
    AND nPostBordaSources>=1
    -- AND NOT ENDS_WITH(query, ' -mature')
),
reqsSample AS (
    SELECT * FROM validRequests
    WHERE RAND() < samplingRate
),
beacon AS (
    SELECT DISTINCT
        beacon.guid,
        (SELECT value FROM UNNEST(beacon.properties.key_value) WHERE key = 'mmx_request_uuid') AS mmxRequestUUID,
    FROM `etsy-visit-pipe-prod.canonical.visit_id_beacons`
    WHERE DATE(_PARTITIONTIME) = sampleDate
    AND beacon.event_name = 'search'
),
lfb AS (
    SELECT 
        key listingId, 
        verticaListings_taxonomyPath listingTaxo,
        NULLIF(verticaListings_title, "") listingTitle,
        verticaListings_description  listingDescription,
        verticaListings_tags listingTags,
    FROM `etsy-ml-systems-prod.feature_bank_v2.listing_feature_bank_most_recent`
),
qis AS (
    SELECT 
        query_raw query,
        CASE 
            WHEN class_id = 0 THEN 'broad' 
            WHEN class_id = 1 THEN 'direct_unspecified'
            WHEN class_id = 2 THEN 'direct_specified' 
        END AS qisClass
    FROM `etsy-search-ml-prod.mission_understanding.qis_scores`
),
qlm AS (
    SELECT query_raw query, _date date, bin 
    FROM `etsy-batchjobs-prod.snapshots.query_level_metrics_raw`
    WHERE _date BETWEEN DATE_SUB(sampleDate, INTERVAL 1 DAY) AND DATE_ADD(sampleDate, INTERVAL 1 DAY)
),
qfb AS (
  SELECT key query, queryTaxoClassification_taxoPath queryTaxo
  FROM `etsy-ml-systems-prod.feature_bank_v2.query_feature_bank_most_recent`
),
queryHydratedRequests AS (
    SELECT
        etsyUUID,
        mmxRequestUUID,
        guid,
        query,
        REGEXP_CONTAINS(query, '(\?i)\\bgift|\\bfor (\\bhim|\\bher|\\bmom|\\bdad|\\bmother|\\bfather|\\bdaughter|\\bson|\\bwife|\\bhusband|\\bpartner|\\baunt|\\buncle|\\bniece|\\bnephew|\\bfiance|\\bcousin|\\bin law|\\bboyfriend|\\bgirlfriend|\\bgrand|\\bfriend|\\bbest friend)') isGift,
        IFNULL(qlm.bin, 'novel') queryBin,
        IFNULL(qis.qisClass, 'missing') qisClass,
        queryTaxo,
        requestSource platform,
        userLanguage,
        numResults,
        matureFilter,
        reqsSample.date,
        ARRAY_CONCAT(
            ARRAY(
                SELECT STRUCT(listing_id AS listingId, idx AS rankingRank, CAST(NULL AS INT64) AS retrievalRank, CAST(NULL AS STRING) AS retrievalSrc, CAST(NULL AS INT64) AS bordaRank, 1 AS pageNum)
                FROM UNNEST(candidateSources) cs,
                    UNNEST(cs.listingIds) AS listing_id WITH OFFSET idx
                WHERE cs.stage = "MO_LASTPASS"
                AND idx < listingPerPage
            ),
            ARRAY(
                SELECT STRUCT(listing_id AS listingId, idx AS rankingRank, CAST(NULL AS INT64) AS retrievalRank, CAST(NULL AS STRING) AS retrievalSrc, CAST(NULL AS INT64) AS bordaRank, 2 AS pageNum)
                FROM UNNEST(candidateSources) cs,
                    UNNEST(cs.listingIds) AS listing_id WITH OFFSET idx
                WHERE cs.stage = "MO_LASTPASS"
                AND idx >= listingPerPage AND idx < listingPerPage * 2
                ORDER BY RAND()
                LIMIT 10
            ),
            ARRAY(
                SELECT STRUCT(listing_id AS listingId, idx AS rankingRank, CAST(NULL AS INT64) AS retrievalRank, CAST(NULL AS STRING) AS retrievalSrc, CAST(NULL AS INT64) AS bordaRank, 3 AS pageNum)
                FROM UNNEST(candidateSources) cs,
                    UNNEST(cs.listingIds) AS listing_id WITH OFFSET idx
                WHERE cs.stage = "MO_LASTPASS"
                AND idx >= listingPerPage * 2 AND idx < listingPerPage * 3
                ORDER BY RAND()
                LIMIT 10
            ),
            ARRAY(
                SELECT STRUCT(listing_id AS listingId, CAST(NULL AS INT64) AS rankingRank, idx AS retrievalRank, cs.source AS retrievalSrc, CAST(NULL AS INT64) AS bordaRank, CAST(NULL AS INT64) AS pageNum)
                FROM UNNEST(candidateSources) cs,
                    UNNEST(cs.listingIds) AS listing_id WITH OFFSET idx
                WHERE cs.stage = "POST_FILTER"
                ORDER BY RAND()
                LIMIT 10
            ),
            ARRAY(
                SELECT STRUCT(listing_id AS listingId, CAST(NULL AS INT64) AS rankingRank, CAST(NULL AS INT64) AS retrievalRank, CAST(NULL AS STRING) AS retrievalSrc, idx AS bordaRank, CAST(NULL AS INT64) AS pageNum)
                FROM UNNEST(candidateSources) cs,
                    UNNEST(cs.listingIds) AS listing_id WITH OFFSET idx
                WHERE cs.stage = "POST_BORDA"
                ORDER BY RAND()
                LIMIT 10
            )
        ) listingSamples
    FROM reqsSample
    LEFT JOIN qlm USING (query, date)
    LEFT JOIN qis USING (query)
    LEFT JOIN qfb USING (query)
    LEFT JOIN beacon USING (mmxRequestUUID)
), 
flatQueryHydratedRequests AS (
    SELECT * EXCEPT (listingSamples)
    FROM queryHydratedRequests,
        UNNEST(queryHydratedRequests.listingSamples) listingSample
    WHERE guid IS NOT NULL
)
SELECT 
    flatQueryHydratedRequests.*,
    lfb.listingTitle,
    lfb.listingDescription,
    lfb.listingTaxo,
    lfb.listingTags,
FROM flatQueryHydratedRequests
LEFT JOIN lfb USING(listingId)
WHERE listingTitle IS NOT NULL;



create or replace table `etsy-sr-etl-prod.yzhang.sem_rel_test_sample` as (
  SELECT * FROM `etsy-data-warehouse-prod.search.sem_rel_get_requests`('2024-04-21', 88, 0.0006, 'web', 48)
  UNION ALL
  SELECT * FROM `etsy-data-warehouse-prod.search.sem_rel_get_requests`('2024-04-21', 74, 0.0006, 'mweb', 34)
  UNION ALL
  SELECT * FROM `etsy-data-warehouse-prod.search.sem_rel_get_requests`('2024-04-21', 68, 0.0006, 'boe', 28)
)