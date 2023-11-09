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
        AND DATE(queryTime) >= DATE('2023-10-16')
        AND DATE(queryTime) <= DATE('2023-10-18')
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
        from `etsy-ml-systems-prod.feature_bank_v2.query_feature_bank_2023-10-18`
        where queryTaxoDemandFeatures_purchaseTopTaxonomyPaths is not null
        and array_length(queryTaxoDemandFeatures_purchaseTopTaxonomyPaths.list) > 0
        and queryTaxoDemandFeatures_purchaseTopTaxonomyPaths.list[0].element != ""
    ),
    user_data as (
        select `key` as user_id, 
            userSegmentFeatures_buyerSegment as buyer_segment
        from `etsy-ml-systems-prod.feature_bank_v2.user_feature_bank_2023-10-18`
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
        and _date >= DATE('2023-10-16')
        and _date <= DATE('2023-10-18')
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


---- % GMS at risk
-- sanity check
select 
    new_tb.query,
    old.purchase_top_paths,
    old.purchase_top_counts,
    old.listing_top_taxo,
    new_tb.top25
from `etsy-sr-etl-prod.yzhang.query_taxo_lastpass_rpc_cutoff0` new_tb
join `etsy-sr-etl-prod.yzhang.query_taxo_lastpass_rpc` old
on new_tb.mmxRequestUUID = old.mmxRequestUUID
and new_tb.query = old.query
and new_tb.query_date = old.query_date
and new_tb.listingId = old.listingId
and new_tb.userId = old.userId
and new_tb.page_no = old.page_no
and new_tb.winsorized_gms = old.winsorized_gms


SELECT sum(winsorized_gms) 
FROM `etsy-sr-etl-prod.yzhang.query_taxo_lastpass_rpc_cutoff0`
where top40 = 'remove'


---- maximum GMS gain
-- aggregate gms on level 2 taxonomy
-- CREATE OR REPLACE TABLE `etsy-sr-etl-prod.yzhang.query_taxo_lastpass_rpc_agg_gms` AS (
--     with tmp as (
--         select *
--         from `etsy-sr-etl-prod.yzhang.query_taxo_lastpass_rpc`
--         where winsorized_gms is not null
--         and listing_second_taxo is not null
--         and query is not null
--     ),
--     agg_gms_data as (
--         SELECT 
--             query, 
--             listing_second_taxo as level2_taxo, 
--             avg(winsorized_gms) as agg_gms
--             -- max(winsorized_gms)
--             -- PERCENTILE_DISC(winsorized_gms, 0.5) OVER(partition by query, listing_second_taxo) as agg_gms
--         FROM tmp
--         group by query, listing_second_taxo
--     )
--     select distinct * from agg_gms_data
-- )

CREATE OR REPLACE TABLE `etsy-sr-etl-prod.yzhang.query_taxo_lastpass_rpc_agg_gms` AS (
    with query_listing_gms as (
        select 
            p.query, p.listingId, p.total_winsorized_gms, 
            lv.full_path,
            split(lv.full_path, '.')[safe_offset(0)] as top_category, 
            split(lv.full_path, '.')[safe_offset(1)] as second_category, 
        from `etsy-data-warehouse-prod.propensity.adjusted_query_listing_pairs` p
        join `etsy-data-warehouse-prod.listing_mart.listing_vw` lv
        on p.listingId = lv.listing_id
        where platform = 'web' and region = 'US' and language = 'en-US'
        and _date >= DATE('2023-10-16')
        and _date <= DATE('2023-10-18')
    ),
    qlg_taxo as (
        select 
            query, listingId, total_winsorized_gms, 
            if (top_category is not null and second_category is not null, concat(top_category, '.', second_category), null) as listing_second_taxo
        from query_listing_gms
    ),
    qlg_taxo_clean as (
        select * from qlg_taxo
        where query is not null
        and total_winsorized_gms is not null
        and listing_second_taxo is not null
    )
    select query, listing_second_taxo as level2_taxo, max(total_winsorized_gms) as agg_gms
    from qlg_taxo_clean
    group by query, listing_second_taxo
)


CREATE OR REPLACE TABLE `etsy-sr-etl-prod.yzhang.query_taxo_lastpass_rpc_taxo_ancenstor_raw` AS (
    with fb_data as (
        select 
            `key` as query,
            queryTaxoDemandFeatures_purchaseLevel2TaxonomyPaths as level2_path_raw
        from `etsy-ml-systems-prod.feature_bank_v2.query_feature_bank_2023-10-18`
        where `key` in (
            select query from `etsy-sr-etl-prod.yzhang.query_taxo_lastpass_rpc`
        )
    )
    select query, level2_path_raw, level2_path
    from fb_data, unnest(level2_path_raw.list) as level2_path WITH OFFSET pos  
    order by query, pos
)

-- run dataflow query_taxo_ancestor_generation

CREATE OR REPLACE TABLE `etsy-sr-etl-prod.yzhang.query_taxo_lastpass_rpc_gms_replacement` AS (
    select 
        ancestor.query,
        ancestor.level2_path as listing_level2_taxo, 
        ag.agg_gms as gms_replacement
    from `etsy-sr-etl-prod.yzhang.query_taxo_lastpass_rpc_taxo_ancenstor` ancestor
    left join `etsy-sr-etl-prod.yzhang.query_taxo_lastpass_rpc_agg_gms` ag
    on ancestor.query = ag.query
    and ancestor.level2_ancestor = ag.level2_taxo
)


create or replace table `etsy-sr-etl-prod.yzhang.query_taxo_lastpass_rpc_cutoff0_amp` as (
    select 
        rpc.*,
        gr.gms_replacement,
        gr.gms_replacement - rpc.winsorized_gms as net_gms_gain
    from `etsy-sr-etl-prod.yzhang.query_taxo_lastpass_rpc_cutoff0` rpc
    left join `etsy-sr-etl-prod.yzhang.query_taxo_lastpass_rpc_gms_replacement` gr
    on rpc.query = gr.query
    and rpc.listing_second_taxo = gr.listing_level2_taxo
)
-- same for cutoff 2 and cutoff 5

select winsorized_gms, gms_replacement, net_gms_gain
from `etsy-sr-etl-prod.yzhang.query_taxo_lastpass_rpc_cutoff0_amp`
where net_gms_gain is not null

SELECT sum(net_gms_gain) 
FROM `etsy-sr-etl-prod.yzhang.query_taxo_lastpass_rpc_cutoff0_amp`
where second0 = 'remove'


with rpc_query as (
    select distinct query
    from `etsy-sr-etl-prod.yzhang.query_taxo_lastpass_rpc`
    where winsorized_gms is not null
),
rpc_qtd_feature as (
    select rpc_query.query, queryTaxoDemandFeatures_purchaseLevel2TaxonomyPaths.list as purchase_level2_paths,
    from rpc_query
    join `etsy-ml-systems-prod.feature_bank_v2.query_feature_bank_2023-10-18` fb_data
    on rpc_query.query = fb_data.key
)
select count(distinct mmxRequestUUID)
from `etsy-sr-etl-prod.yzhang.query_taxo_lastpass_rpc`
where query in (
    select distinct query
    from rpc_qtd_feature
    where array_length(purchase_level2_paths) > 1
)

-- 10747169 distinct queries
-- 1801948 queries with gms
-- among that 84014 queries have > 1 taxos

-- 42948231 requests
-- 5565816 (13%) have query with > 1 taxos