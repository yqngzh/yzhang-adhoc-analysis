-- take a day of RPC log, get requests on wsg
-- join query taxo demand features that are not null
-- join fl data to get interactions
CREATE OR REPLACE TABLE `etsy-sr-etl-prod.yzhang.query_taxo_distrib_match_2023_09_24` AS (
    with rpc_data as (
        SELECT
            response.mmxRequestUUID,
            request.query,
            request.options.personalizationOptions.userId,
            CAST(request.offset / request.limit + 1 AS INTEGER) page_no,
            listingId,
            position,
        FROM `etsy-searchinfra-gke-prod-2.thrift_mmx_listingsv2search_search.rpc_logs_*`,
            UNNEST(response.listingIds) AS listingId  WITH OFFSET position
        WHERE request.options.searchPlacement = "wsg"
        AND DATE(queryTime) = DATE('2023-09-24')
        AND request.options.csrOrganic = TRUE
        -- AND (request.offset + request.limit) < 144
        AND request.options.mmxBehavior.matching IS NOT NULL
        AND request.options.mmxBehavior.ranking IS NOT NULL
        AND request.options.mmxBehavior.marketOptimization IS NOT NULL
    ),
    query_taxo_feature as (
        select `key` as query_str, 
            queryLevelMetrics_bin as query_bin,
            queryTaxoDemandFeatures_clickTopTaxonomyPaths as click_top_paths,
            queryTaxoDemandFeatures_clickTopTaxonomyCounts as click_top_counts,
            queryTaxoDemandFeatures_clickLevel2TaxonomyPaths as click_level2_paths,
            queryTaxoDemandFeatures_clickLevel2TaxonomyCounts as click_level2_counts,
            queryTaxoDemandFeatures_purchaseTopTaxonomyPaths as purchase_top_paths,
            queryTaxoDemandFeatures_purchaseTopTaxonomyCounts as purchase_top_counts,
            queryTaxoDemandFeatures_purchaseLevel2TaxonomyPaths as purchase_level2_paths,
            queryTaxoDemandFeatures_purchaseLevel2TaxonomyCounts as purchase_level2_counts,
        from `etsy-ml-systems-prod.feature_bank_v2.query_feature_bank_most_recent`
        where queryTaxoDemandFeatures_clickTopTaxonomyPaths is not null
    )
    select 
        rpc_data.*, 
        lt.full_path as listing_taxo_full_path,
        click_top_paths, click_top_counts,
        click_level2_paths, click_level2_counts,
        purchase_top_paths, purchase_top_counts,
        purchase_level2_paths, purchase_level2_counts,
        IF ("click" in UNNEST(q.attributions), 1, 0) clicked,
        IF ("purchase" in UNNEST(q.attributions), 1, 0) purchased,
        query_bin
    from rpc_data
    join `etsy-data-warehouse-prod.materialized.listing_taxonomy` lt
    on rpc_data.listingId = lt.listing_id
    join query_taxo_feature
    on rpc_data.query = query_taxo_feature.query_str   
    LEFT JOIN `etsy-ml-systems-prod.attributed_instance.query_pipeline_web_organic_2023_09_24` q
    ON rpc_data.mmxRequestUUID = q.requestUUID
    AND listingId = q.candidateInfo.docInfo.listingInfo.listingId
)

-- add user segment
CREATE OR REPLACE TABLE `etsy-sr-etl-prod.yzhang.query_taxo_distrib_match_full_2023_09_24` AS (
    select 
        distrib_match.*, 
        IF (distrib_match.userId > 0, user_fb.userSegmentFeatures_buyerSegment, "Signed Out") buyer_segment
    from `etsy-sr-etl-prod.yzhang.query_taxo_distrib_match_2023_09_24` distrib_match
    left join `etsy-ml-systems-prod.feature_bank_v2.user_feature_bank_most_recent` user_fb
    on distrib_match.userId = user_fb.key
)

with request_length_data as (
  SELECT mmxRequestUUID, count(*) as requestLength 
  FROM `etsy-sr-etl-prod.yzhang.query_taxo_distrib_match_full_2023_09_24`
  group by mmxRequestUUID
)
select count(*) from request_length_data
where requestLength != 48 and requestLength != 34
-- 9641990 requests, 43544 not standard length (0.5%)

-- remove pages with missing data
CREATE OR REPLACE TABLE `etsy-sr-etl-prod.yzhang.query_taxo_distrib_match_clean_2023_09_24` AS (
    with request_length_data as (
        SELECT mmxRequestUUID, count(*) as requestLength 
        FROM `etsy-sr-etl-prod.yzhang.query_taxo_distrib_match_full_2023_09_24`
        group by mmxRequestUUID
    ),
    not_standard_len as (
        select distinct mmxRequestUUID  from request_length_data
        where requestLength != 48 and requestLength != 34
    )
    select * from `etsy-sr-etl-prod.yzhang.query_taxo_distrib_match_full_2023_09_24`
    where mmxRequestUUID not in (
        select mmxRequestUUID from not_standard_len
    )
)
