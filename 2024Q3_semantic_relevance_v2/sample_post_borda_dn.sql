CREATE OR REPLACE TABLE FUNCTION `etsy-search-ml-dev.yzhang.sample_post_borda_dn`(
    sampleDate DATE,
    minResults INT64,
    samplingRate FLOAT64,
    samplePlatform STRING, 
    listingPerPage INT64
) AS
WITH allRequests AS (
    SELECT
        (SELECT COUNTIF(stage='POST_BORDA') FROM UNNEST(OrganicRequestMetadata.candidateSources)) nPostBordaSources,
        (SELECT COUNTIF(stage='POST_SEM_REL_FILTER') FROM UNNEST(OrganicRequestMetadata.candidateSources)) nPostSemrelSources,
        COALESCE((SELECT NULLIF(query, '') FROM UNNEST(request.filter.query.translations) WHERE language = 'en'), NULLIF(request.query, '')) query,
        request.options.userLanguage userLanguage,
        request.options.userCountry userCountry,
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
    AND request.options.interleavingConfig IS NULL
    AND response.count >= minResults
), 
validRequests AS (
    SELECT *
    FROM allRequests
    WHERE requestSource = samplePlatform
    AND userCountry = 'US'
    AND (nPostBordaSources>=1 OR nPostSemrelSources>=1)
),
reqsSample AS (
    SELECT * FROM validRequests
    WHERE RAND() < samplingRate
),
lfb AS (
    SELECT 
        key AS listingId,
        COALESCE(NULLIF(verticaListings_title, ''), NULLIF(verticaListingTranslations_machineTranslatedEnglishTitle, '')) listingTitle,
        IFNULL(verticaListings_description, "") listingDescription,
        (SELECT STRING_AGG(element, ';') FROM UNNEST(descNgrams_ngrams.list)) AS listingDescNgrams,
        IFNULL(verticaListings_taxonomyPath, "") listingTaxo,
        (SELECT STRING_AGG(element, ';') FROM UNNEST(kbAttributesV2_sellerAttributesV2.list)) AS listingAttributes,
        IFNULL(verticaListings_tags, "") listingTags,
        IFNULL(verticaSellerBasics_shopName, "") shopName,
    FROM `etsy-ml-systems-prod.feature_bank_v2.listing_feature_bank_2024-11-05`
),
queryHydratedRequests AS (
    SELECT
        etsyUUID,
        mmxRequestUUID,
        query,
        requestSource platform,
        userLanguage,
        userCountry,
        reqsSample.date,
        ARRAY(
            SELECT STRUCT(listing_id AS listingId, idx AS bordaRank)
            FROM UNNEST(candidateSources) cs,
                UNNEST(cs.listingIds) AS listing_id WITH OFFSET idx
            WHERE cs.stage = IF(nPostSemrelSources>0, "POST_SEM_REL_FILTER", "POST_BORDA")
            ORDER BY RAND()
            LIMIT 10
        ) listingSamples
    FROM reqsSample
), 
flatQueryHydratedRequests AS (
    SELECT * EXCEPT (listingSamples)
    FROM queryHydratedRequests,
        UNNEST(queryHydratedRequests.listingSamples) listingSample
)
SELECT 
    flatQueryHydratedRequests.*,
    lfb.listingTitle,
    lfb.listingDescription,
    lfb.listingDescNgrams,
    lfb.listingTaxo,
    lfb.listingTags,
    lfb.listingAttributes,
    lfb.shopName
FROM flatQueryHydratedRequests
LEFT JOIN lfb USING(listingId)
WHERE listingTitle != ''
AND listingDescNgrams != ''
AND shopName != ''
AND query != '';
