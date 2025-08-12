DECLARE start_date DATE DEFAULT "2025-06-03";
DECLARE end_date DATE DEFAULT "2025-06-23";
DECLARE n_rows INT64 DEFAULT 10000000;

WITH rpc_data as (
  SELECT
    response.mmxRequestUUID as mmxRequestUUID,
    DATE(queryTime) as date,
    COALESCE(request.query, '') as query,
  FROM `etsy-searchinfra-gke-prod-2.thrift_mmx_listingsv2search_search.rpc_logs_*`
  WHERE request.OPTIONS.cacheBucketId LIKE "live%"
  AND request.options.csrOrganic
  AND request.query <> ''
  AND request.offset = 0
  AND DATE(queryTime) BETWEEN start_date AND end_date
),
modelLabels AS (
  SELECT
    ROW_NUMBER() OVER (ORDER BY qlm.tableUUID DESC) as rowNum,
    qlm.date,
    qlm.tableUUID,
    classId gold_label,
    qlm.softmaxScores gold_probas,
    dr.query query,
    rpc_data.query rpcQuery,
    listingId listingId,
    listingTitle title,
    listingTags tags,
    listingTaxo taxonomyPath,
    userCountry userCountry,
    userLanguage preferredLanguage
  FROM `etsy-data-warehouse-prod.search.sem_rel_query_listing_metrics` qlm
  JOIN `etsy-data-warehouse-prod.search.sem_rel_hydrated_daily_requests` dr USING (date, tableUUID)
  JOIN rpc_data USING (date, mmxRequestUUID)
  WHERE qlm.date BETWEEN start_date AND end_date 
  AND modelName='bert-cern-l24-h1024-a16'
),
lfb AS (
  SELECT
    key listingId,
    IFNULL(
        COALESCE(NULLIF(verticaListings_title, ""), verticaListingTranslations_primaryLanguageTitle),
        ""
    ) title,
    IFNULL(verticaListings_description, "") description,
    IFNULL(verticaListings_tags, "") tags,
    IFNULL(verticaListings_taxonomyPath, "") taxonomyPath,
    (SELECT STRING_AGG(element, ';') FROM UNNEST(kbAttributesV2_sellerAttributesV2.list)) AS listingAttributes,
    ARRAY(SELECT element FROM UNNEST(topQueryYear_clickTopQuery.list)) clickTopQuery,
    ARRAY(SELECT element FROM UNNEST(topQueryYear_cartTopQuery.list)) cartTopQuery,
    ARRAY(SELECT element FROM UNNEST(topQueryYear_purchaseTopQuery.list)) purchaseTopQuery,
    IFNULL(verticaSellerBasics_shopName, "") shopName,
    (SELECT STRING_AGG(element, '.') FROM UNNEST(descNgrams_ngrams.list)) AS descNgrams,
    IFNULL(verticaShopSettings_primaryLanguage, "") shop_primaryLanguage,
    IFNULL(localeFeatures_listingCountry, "") listingCountry
  FROM `etsy-ml-systems-prod.feature_bank_v2.listing_feature_bank_most_recent`
)
SELECT modelLabels.* EXCEPT (rpcQuery, rowNum),shopName
FROM modelLabels
LEFT JOIN lfb USING(listingId)
WHERE modelLabels.rowNum <= n_rows
ORDER BY modelLabels.tableUUID DESC
