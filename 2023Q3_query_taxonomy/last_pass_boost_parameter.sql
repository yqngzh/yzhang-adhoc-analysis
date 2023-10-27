CREATE OR REPLACE TABLE `etsy-sr-etl-prod.yzhang.query_taxo_lastpass_rpc` AS (
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
        AND DATE(queryTime) >= DATE('2023-10-21')
        AND DATE(queryTime) <= DATE('2023-10-23')
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
        and array_length(queryTaxoDemandFeatures_purchaseTopTaxonomyPaths.list) > 0
        and queryTaxoDemandFeatures_purchaseTopTaxonomyPaths.list[0].element != ""
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
    ),
    query_listing_gms as (
        select * 
        from `etsy-data-warehouse-prod.propensity.adjusted_query_listing_pairs`
        where platform = 'web' and region = 'US' and language = 'en-US'
        and _date >= DATE('2023-10-21')
        and _date <= DATE('2023-10-23')
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
        qlg.total_winsorized_gms as winsorized_gms
    from rpc_data
    left join query_taxo_data q
    on rpc_data.query = q.query_str
    left join listing_data l
    on rpc_data.listingId = l.listing_id
    left join user_data u 
    on rpc_data.userId = u.user_id
    left join query_listing_gms qlg
    on (
        qlg._date = rpc_data.query_date and 
        qlg.query = rpc_data.query and
        qlg.listingId = rpc_data.listingId
    )
)

-- sanity check
select 
    old.purchase_top_paths,
    old.purchase_top_counts,
    old.listing_top_taxo,
    new_tb.top0
from `etsy-sr-etl-prod.yzhang.query_taxo_lastpass_rpc_analysis` new_tb
join `etsy-sr-etl-prod.yzhang.query_taxo_lastpass_rpc` old
on new_tb.mmxRequestUUID = old.mmxRequestUUID
and new_tb.query = old.query
and new_tb.query_date = old.query_date
and new_tb.listingId = old.listingId
and new_tb.userId = old.userId
and new_tb.page_no = old.page_no
and new_tb.winsorized_gms = old.winsorized_gms


SELECT sum(winsorized_gms) 
FROM `etsy-sr-etl-prod.yzhang.query_taxo_lastpass_rpc_analysis`
where top40 = 'remove'