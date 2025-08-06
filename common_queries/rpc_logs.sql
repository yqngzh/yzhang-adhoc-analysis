select 
    response.mmxRequestUUID,
    request.query,
    cs.listingIds,
    cs.SemrelScores
FROM `etsy-searchinfra-gke-prod-2.thrift_mmx_listingsv2search_search.rpc_logs_*`,
    unnest(OrganicRequestMetadata.candidateSources) as cs
WHERE request.options.searchPlacement in ("wsg", "wmg", "allsr")
AND DATE(queryTime) = DATE('2025-07-20')
AND request.options.csrOrganic = TRUE
limit 20


with rpc_data as (
    SELECT
        response.mmxRequestUUID,
        request.query,
        request.options.personalizationOptions.userId,
        CAST(request.offset / request.limit + 1 AS INTEGER) page_no,
        listingId,
        position,
        DATE(queryTime) as query_date
    FROM `etsy-searchinfra-gke-prod-2.thrift_mmx_listingsv2search_search.rpc_logs_*`,
        UNNEST(response.listingIds) AS listingId  WITH OFFSET position
    WHERE request.options.searchPlacement = "wsg"
    AND DATE(queryTime) >= DATE('2023-09-28')
    AND DATE(queryTime) <= DATE('2023-09-30')
    AND request.options.csrOrganic = TRUE
    AND (request.offset + request.limit) < 144
    AND request.options.mmxBehavior.matching IS NOT NULL
    AND request.options.mmxBehavior.ranking IS NOT NULL
    AND request.options.mmxBehavior.marketOptimization IS NOT NULL
),
query_taxo_data as (
    select `key` as query_str, 
        queryLevelMetrics_bin as query_bin,
        queryTaxoDemandFeatures_purchaseTopTaxonomyPaths as purchase_top_paths,
        queryTaxoDemandFeatures_purchaseTopTaxonomyCounts as purchase_top_counts,
        queryTaxoDemandFeatures_purchaseLevel2TaxonomyPaths as purchase_level2_paths,
        queryTaxoDemandFeatures_purchaseLevel2TaxonomyCounts as purchase_level2_counts,
    from `etsy-ml-systems-prod.feature_bank_v2.query_feature_bank_most_recent`
    where queryTaxoDemandFeatures_purchaseTopTaxonomyPaths is not null
    and queryTaxoDemandFeatures_purchaseTopTaxonomyPaths.list[0] is not null
),
user_data as (
    select `key` as user_id, 
        userSegmentFeatures_buyerSegment as buyer_segment
    from `etsy-ml-systems-prod.feature_bank_v2.user_feature_bank_most_recent`
),
listing_data as (
    select 
        alb.listing_id, 
        alb.top_category, 
        split(lt.full_path, '.')[safe_offset(1)] as second_category, 
        alb.past_year_gms
    from `etsy-data-warehouse-prod.rollups.active_listing_basics` alb
    join `etsy-data-warehouse-prod.materialized.listing_taxonomy` lt
    on alb.listing_id = lt.listing_id
    and alb.taxonomy_id = lt.taxonomy_id
)
select 
    rpc_data.*,
    q.query_bin,
    if (rpc_data.userId > 0, u.buyer_segment, "Signed Out") as buyer_segment,
    q.purchase_top_paths,
    q.purchase_top_counts,
    q.purchase_level2_paths,
    q.purchase_level2_counts,
    l.top_category as listing_top_taxo,
    if (l.second_category is not null, concat(l.top_category, '.', l.second_category), null) as listing_second_taxo,
    l.past_year_gms as listing_past_year_gms
from rpc_data
left join query_taxo_data q
on rpc_data.query = q.query_str
left join listing_data l
on rpc_data.listingId = l.listing_id
left join user_data u 
on rpc_data.userId = u.user_id


WITH allRequests AS (
    SELECT
        (SELECT COUNTIF(stage='MO_LASTPASS') FROM UNNEST(OrganicRequestMetadata.candidateSources)) nLastpassSources,
        (SELECT COUNTIF(stage='POST_FILTER') FROM UNNEST(OrganicRequestMetadata.candidateSources)) nPostFilterSources,
        (SELECT COUNTIF(stage='POST_BORDA') FROM UNNEST(OrganicRequestMetadata.candidateSources)) nPostBordaSources,
        (SELECT COUNTIF(stage='POST_SEM_REL_FILTER') FROM UNNEST(OrganicRequestMetadata.candidateSources)) nPostSemrelSources,
        COALESCE((SELECT NULLIF(query, '') FROM UNNEST(request.filter.query.translations) WHERE language = 'en'), NULLIF(request.query, '')) query,
        request.options.userLanguage userLanguage,
        request.options.userCountry userCountry,
        response.count numResults,
        request.options.searchPlacement searchPlacement,
        request.filter.maturity as matureFilter,
        DATE(queryTime) date,
        RequestIdentifiers.etsyRequestUUID as etsyUUID,
        response.mmxRequestUUID as mmxRequestUUID,
        RequestIdentifiers.etsyRootUUID as rootUUID,
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
    AND request.options.searchPlacement IN ('wsg', 'allsr', 'wmg')
    AND request.query <> ''
    AND request.offset = 0
    AND DATE(queryTime) = sampleDate
    AND request.options.interleavingConfig IS NULL
    AND response.count >= minResults
    AND NOT EXISTS (
      SELECT * FROM UNNEST(request.context)
      WHERE key = "req_source" AND value = "bot"
    )
),
validRequests AS (
    SELECT *
    FROM allRequests
    WHERE requestSource = samplePlatform
    AND nLastPassSources=1
    AND nPostFilterSources>=1
    AND (nPostBordaSources>=1 OR nPostSemrelSources>=1)
),
reqsSample AS (
    -- US Search
    SELECT * FROM validRequests
    WHERE userCountry = 'US' AND searchPlacement IN ('wsg', 'allsr')
      AND RAND() < samplingRate
    UNION ALL
    -- Intl Search
    SELECT * FROM validRequests
    WHERE userCountry != 'US' AND searchPlacement IN ('wsg', 'allsr')
      AND RAND() < samplingRate
    UNION ALL 
    -- US Market
    SELECT * FROM validRequests
    WHERE userCountry = 'US' AND searchPlacement = 'wmg'
      AND RAND() < samplingRate
    UNION ALL
    -- Intl Market
    SELECT * FROM validRequests
    WHERE userCountry != 'US' AND searchPlacement = 'wmg'
      AND RAND() < samplingRate
),
prolistRequests AS (
  SELECT
    RequestIdentifiers.etsyRootUUID AS rootUUID,
    response.listingIds AS prolist_listingIds
  FROM `etsy-searchinfra-gke-prod-2.thrift_mmx_listingsv2search_search.rpc_logs_*`
  WHERE
    DATE(queryTime) = sampleDate
    AND request.options.cacheBucketId LIKE "live%"
    AND request.options.queryType = 'prolist'
),
reqsSampleWithProlist AS (
  SELECT *
  FROM reqsSample
  LEFT JOIN prolistRequests
    USING(rootUUID)
),
beacon AS (
    SELECT DISTINCT
        beacon.guid,
        visit_id,
        COALESCE(
            (SELECT value FROM UNNEST(beacon.properties.key_value) WHERE key = 'mmx_request_uuid') ,
            (SELECT JSON_KEYS(PARSE_JSON(value))[SAFE_OFFSET(0)] FROM UNNEST(beacon.properties.key_value) WHERE key = 'mmx_request_uuid_map')) mmxRequestUUID
    FROM `etsy-visit-pipe-prod.canonical.visit_id_beacons`
    WHERE DATE(_PARTITIONTIME) = sampleDate
    AND beacon.event_name IN ('search', 'market')
),
lfb AS (
    SELECT
        key listingId,
        verticaListings_taxonomyPath listingTaxo,
        COALESCE(NULLIF(verticaListings_title, ''), NULLIF(verticaListingTranslations_machineTranslatedEnglishTitle, '')) listingTitle,
        verticaListings_description listingDescription,
        verticaListings_tags listingTags,
        (SELECT STRING_AGG(element, ';') FROM UNNEST(kbAttributesV2_sellerAttributesV2.list)) AS listingAttributes,
        verticaSellerBasics_shopName as listingShopName,
    FROM `etsy-ml-systems-prod.feature_bank_v2.listing_feature_bank_most_recent`
),
qis AS (
    SELECT DISTINCT
        query,
        CASE
          WHEN prediction = 0 THEN 'broad'
          WHEN prediction = 1 THEN 'direct_unspecified'
          WHEN prediction = 2 THEN 'direct_specified'
        END AS qisClass
    FROM `etsy-search-ml-prod.mission_understanding.qis_scores_v2`
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
        userCountry,
        numResults,
        matureFilter,
        searchPlacement,
        reqsSampleWithProlist.date,
        ARRAY_CONCAT(
            ARRAY(
                SELECT STRUCT(
                    "organic" AS resultType,
                    listing_id AS listingId,
                    idx AS rankingRank,
                    CAST(NULL AS INT64) AS retrievalRank,
                    CAST(NULL AS STRING) AS retrievalSrc,
                    CAST(NULL AS INT64) AS bordaRank,
                    1 AS pageNum)
                FROM UNNEST(candidateSources) cs,
                    UNNEST(cs.listingIds) AS listing_id WITH OFFSET idx
                WHERE cs.stage = "MO_LASTPASS"
                AND idx < listingPerPage
            ),
            ARRAY(
                SELECT STRUCT(
                    "organic" AS resultType,
                    listing_id AS listingId,
                    idx AS rankingRank,
                    CAST(NULL AS INT64) AS retrievalRank,
                    CAST(NULL AS STRING) AS retrievalSrc,
                    CAST(NULL AS INT64) AS bordaRank,
                    2 AS pageNum)
                FROM UNNEST(candidateSources) cs,
                    UNNEST(cs.listingIds) AS listing_id WITH OFFSET idx
                WHERE cs.stage = "MO_LASTPASS"
                AND idx >= listingPerPage AND idx < listingPerPage * 2
                ORDER BY RAND()
                LIMIT 10
            ),
            ARRAY(
                SELECT STRUCT(
                    "organic" AS resultType,
                    listing_id AS listingId,
                    idx AS rankingRank,
                    CAST(NULL AS INT64) AS retrievalRank,
                    CAST(NULL AS STRING) AS retrievalSrc,
                    CAST(NULL AS INT64) AS bordaRank,
                    3 AS pageNum)
                FROM UNNEST(candidateSources) cs,
                    UNNEST(cs.listingIds) AS listing_id WITH OFFSET idx
                WHERE cs.stage = "MO_LASTPASS"
                AND idx >= listingPerPage * 2 AND idx < listingPerPage * 3
                ORDER BY RAND()
                LIMIT 10
            ),
            ARRAY(
                SELECT STRUCT(
                    "organic" AS resultType,
                    listing_id AS listingId,
                    CAST(NULL AS INT64) AS rankingRank,
                    idx AS retrievalRank,
                    cs.source AS retrievalSrc,
                    CAST(NULL AS INT64) AS bordaRank,
                    CAST(NULL AS INT64) AS pageNum)
                FROM UNNEST(candidateSources) cs,
                    UNNEST(cs.listingIds) AS listing_id WITH OFFSET idx
                WHERE cs.stage = "POST_FILTER"
                ORDER BY RAND()
                LIMIT 10
            ),
            ARRAY(
                SELECT STRUCT(
                    "organic" AS resultType,
                    listing_id AS listingId,
                    CAST(NULL AS INT64) AS rankingRank,
                    CAST(NULL AS INT64) AS retrievalRank,
                    CAST(NULL AS STRING) AS retrievalSrc,
                    idx AS bordaRank,
                    CAST(NULL AS INT64) AS pageNum)
                FROM UNNEST(candidateSources) cs,
                    UNNEST(cs.listingIds) AS listing_id WITH OFFSET idx
                WHERE cs.stage = IF(nPostSemrelSources>0, "POST_SEM_REL_FILTER", "POST_BORDA")
                ORDER BY RAND()
                LIMIT 10
            ),
            -- Prolist
            ARRAY(
                SELECT STRUCT(
                  "prolist" AS resultType,
                  listing_id AS listingId,
                  idx AS rankingRank,
                  CAST(NULL AS INT64) AS retrievalRank,
                  CAST(NULL AS STRING) AS retrievalSrc,
                  CAST(NULL AS INT64) AS bordaRank,
                  1 AS pageNum
                )
                FROM UNNEST(prolist_listingIds) AS listing_id WITH OFFSET idx
                WHERE idx < adsPerPage
            )
        ) listingSamples
    FROM reqsSampleWithProlist
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
        OR searchPlacement = 'wmg' -- market events are all missing guid
),
outputTable AS (
    SELECT
        flatQueryHydratedRequests.*,
        lfb.listingTitle,
        lfb.listingDescription,
        lfb.listingTaxo,
        lfb.listingTags,
        lfb.listingAttributes,
        lfb.listingShopName,
    FROM flatQueryHydratedRequests
    LEFT JOIN lfb USING(listingId)
    WHERE listingTitle IS NOT NULL
)
SELECT
    GENERATE_UUID() AS tableUUID,
    etsyUUID,
    mmxRequestUUId,
    guid,
    visit_id,
    query,
    isGift,
    queryBin,
    qisClass,
    queryTaxo,
    platform,
    userLanguage,
    numResults,
    matureFilter,
    date,
    listingid,
    rankingRank,
    retrievalRank,
    retrievalSrc,
    bordaRank,
    pageNum,
    listingTitle,
    listingDescription,
    listingTaxo,
    listingTags,
    userCountry,
    listingAttributes,
    listingShopName,
    resultType,
    searchPlacement
FROM outputTable