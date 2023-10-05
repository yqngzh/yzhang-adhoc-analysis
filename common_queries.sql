-- training data
select
    requestUUID, visitId, position,
    attributions,
    ctx.docInfo.queryInfo.query, 
    ctx.docInfo.queryInfo.queryLevelMetrics.*,
    ctx.docInfo.queryInfo.queryTaxoDemandFeatures.*,
from `etsy-ml-systems-prod.attributed_instance.query_pipeline_web_organic_2023_10_03`, unnest(contextualInfo) as ctx
where ctx.docInfo.queryInfo.query in ('personalized gift', 'personalise gift', 'mother day', 'mothers day', 'gift for him', 'gifts for him', 'gift')
order by requestUUID, position

-- feature bank
SELECT key, queryLevelMetrics_bin, queryLevelMetrics_gms 
FROM `etsy-ml-systems-prod.feature_bank_v2.query_feature_bank_most_recent`

--- rpc log
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
