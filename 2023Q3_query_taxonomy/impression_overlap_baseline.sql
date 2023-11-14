----  1. Create query level2 purchased taxo features
create or replace table `etsy-sr-etl-prod.yzhang.qtd_query_fb` as (
    with fb_data as (
        select 
            `key` as query, queryLevelMetrics_bin as query_bin,
            ppaths.element as ppaths
        from `etsy-ml-systems-prod.feature_bank_v2.query_feature_bank_2023-11-12`,
            unnest(queryTaxoDemandFeatures_purchaseLevel2TaxonomyPaths.list) as ppaths
    ),
    fb_data_agg as (
        select query, query_bin, array_agg(ppaths) as ppath_level2
        from fb_data
        group by query, query_bin
    ),
    query_intent_data as (
        select query_raw, inference.label as query_intent
        from `etsy-data-warehouse-prod.arizona.query_intent_labels`
        QUALIFY ROW_NUMBER() OVER(PARTITION BY query_raw ORDER BY inference.confidence DESC) = 1
    )
    select fb.query, fb.query_bin, qi.query_intent, fb.ppath_level2
    from fb_data_agg fb
    left join query_intent_data qi
    on fb.query = qi.query_raw
)

-- sanity check number of query
-- with fb_data as (
--     select 
--         `key` as query, ppaths.element as ppaths
--     from `etsy-ml-systems-prod.feature_bank_v2.query_feature_bank_2023-11-12`,
--         unnest(queryTaxoDemandFeatures_purchaseLevel2TaxonomyPaths.list) as ppaths
-- ),
-- fb_data_agg as (
--     select query, array_agg(ppaths) as ppath_level2
--     from fb_data
--     group by query
-- )
-- select count(*) from fb_data_agg
-- 5956161 for both datasets

-- sanity check: Look at first few queries & spot check 'mother day', 'gift', 'desk'
-- with tmp as (
--     select `key` as query, queryTaxoDemandFeatures_purchaseLevel2TaxonomyPaths
--     from `etsy-ml-systems-prod.feature_bank_v2.query_feature_bank_2023-11-12`
--     -- where `key` = 'mother day'
-- )
-- select tmp.*, qtd.ppath_level2
-- from tmp
-- join `etsy-sr-etl-prod.yzhang.qtd_query_fb` qtd
-- on tmp.query = qtd.query
-- -- limit 40


----  2. Create listing level2 purchased taxo features
create or replace table `etsy-sr-etl-prod.yzhang.qtd_listing_fb` as (
    with fb_data as (
        SELECT 
            `key` as listing_id, 
            split(verticaListings_taxonomyPath, ".")[SAFE_OFFSET(0)] as top_node,
            split(verticaListings_taxonomyPath, ".")[SAFE_OFFSET(1)] as second_node,
        FROM `etsy-ml-systems-prod.feature_bank_v2.listing_feature_bank_2023-11-12`
    )
    select 
        listing_id,
        if (top_node is not null and second_node is not null, concat(top_node, '.', second_node), null) as listing_taxo_level2
    from fb_data
)

-- Spot check listings 
-- with tmp as (
--     SELECT `key` as listing_id, verticaListings_taxonomyPath as full_path
--     FROM `etsy-ml-systems-prod.feature_bank_v2.listing_feature_bank_2023-11-12`
-- )
-- select tmp.*, qtd.listing_taxo_level2
-- from tmp
-- join `etsy-sr-etl-prod.yzhang.qtd_listing_fb` qtd
-- on tmp.listing_id = qtd.listing_id
-- limit 200


----  3. Control impression match
create or replace table `etsy-sr-etl-prod.yzhang.qtd_rpc_lqoverlap_web` as (
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
        WHERE request.options.searchPlacement in unnest(["wmg", "wsg"])
        AND DATE(queryTime) = DATE('2023-11-12')
        AND request.options.csrOrganic = TRUE
        AND (request.offset + request.limit) < 144
        AND request.options.mmxBehavior.matching IS NOT NULL
        AND request.options.mmxBehavior.ranking IS NOT NULL
        AND request.options.mmxBehavior.marketOptimization IS NOT NULL
    )
    select 
        rpc_data.*,
        query_fb.query_bin,
        query_fb.query_intent,
        query_fb.ppath_level2,
        listing_fb.listing_taxo_level2,
        if (listing_taxo_level2 in unnest(ppath_level2), 1, 0) as overlap
    from rpc_data
    left join `etsy-sr-etl-prod.yzhang.qtd_query_fb` query_fb
    on rpc_data.query = query_fb.query
    left join `etsy-sr-etl-prod.yzhang.qtd_listing_fb` listing_fb
    on rpc_data.listingId = listing_fb.listing_id
)

-- sanity check

-- select count(*)
-- from `etsy-sr-etl-prod.yzhang.qtd_rpc_lqoverlap_web`
-- where overlap is null
-- ## 0; overlap field is not null

-- select count(*)
-- from `etsy-sr-etl-prod.yzhang.qtd_rpc_lqoverlap_web`
-- where (ppath_level2 is null or array_length(ppath_level2) = 0)
-- and overlap = 1
-- ## 0; if there is no query taxonomy, not overlap

-- select count(*)
-- from `etsy-sr-etl-prod.yzhang.qtd_rpc_lqoverlap_web`
-- where listing_taxo_level2 is null
-- and overlap = 1
-- ## 0; if there is no listing taxonomy, not overlap

-- select ppath_level2, listing_taxo_level2, overlap
-- from `etsy-sr-etl-prod.yzhang.qtd_rpc_lqoverlap_web`
-- where overlap = 0
-- limit 200
-- ## spot check result for overlap = 1 or overlap = 0


----  4. Count overlap on first page on each request
select mmxRequestUUID, sum(overlap) as QTD_match, count(*) as total_impression
from `etsy-sr-etl-prod.yzhang.qtd_rpc_lqoverlap_web`
where page_no = 1
group by mmxRequestUUID

-- average n impression match
with tmp as (
    select mmxRequestUUID, sum(overlap) as QTD_match, count(*) as total_impression
    from `etsy-sr-etl-prod.yzhang.qtd_rpc_lqoverlap_web`
    where page_no = 1
    -- and query_bin = 'top.01'
    -- and array_length(ppath_level2) > 0
    group by mmxRequestUUID
)
-- select avg(tmp.QTD_match)
select count(distinct mmxRequestUUID)
from tmp
where total_impression = 48

-- average n impression in top 10
with subset_requests as (
    select mmxRequestUUID
    from `etsy-sr-etl-prod.yzhang.qtd_rpc_lqoverlap_web`
    where page_no = 1
    group by mmxRequestUUID
    having count(*) = 48
),
qtd_sub as (
    select *
    from `etsy-sr-etl-prod.yzhang.qtd_rpc_lqoverlap_web`
    where mmxRequestUUID in (
        select mmxRequestUUID from subset_requests
    )
),
tmp as (
    select mmxRequestUUID, sum(overlap) as QTD_match, count(*) as total_impression
    from qtd_sub
    where page_no = 1
    and position < 10
    and query_bin = 'top.01'
    -- and array_length(ppath_level2) > 0
    group by mmxRequestUUID
)
-- select avg(tmp.QTD_match)
select count(distinct mmxRequestUUID)
from tmp


----  5. Repeat for BoE
create or replace table `etsy-sr-etl-prod.yzhang.qtd_rpc_lqoverlap_boe` as (
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
        WHERE request.options.searchPlacement = "allsr"
        AND DATE(queryTime) = DATE('2023-11-12')
        AND request.options.csrOrganic = TRUE
        AND (request.offset + request.limit) < 84
        AND request.options.mmxBehavior.matching IS NOT NULL
        AND request.options.mmxBehavior.ranking IS NOT NULL
        AND request.options.mmxBehavior.marketOptimization IS NOT NULL
    )
    select 
        rpc_data.*,
        query_fb.query_bin,
        query_fb.query_intent,
        query_fb.ppath_level2,
        listing_fb.listing_taxo_level2,
        if (listing_taxo_level2 in unnest(ppath_level2), 1, 0) as overlap
    from rpc_data
    left join `etsy-sr-etl-prod.yzhang.qtd_query_fb` query_fb
    on rpc_data.query = query_fb.query
    left join `etsy-sr-etl-prod.yzhang.qtd_listing_fb` listing_fb
    on rpc_data.listingId = listing_fb.listing_id
)

-- average n impression match
with tmp as (
    select mmxRequestUUID, sum(overlap) as QTD_match, count(*) as total_impression
    from `etsy-sr-etl-prod.yzhang.qtd_rpc_lqoverlap_boe`
    where page_no = 1
    -- and query_bin = 'top.01'
    -- and array_length(ppath_level2) > 0
    group by mmxRequestUUID
)
-- select avg(tmp.QTD_match)
select count(distinct mmxRequestUUID)
from tmp
where total_impression = 28

-- average n impression in top 10
with subset_requests as (
    select mmxRequestUUID
    from `etsy-sr-etl-prod.yzhang.qtd_rpc_lqoverlap_boe`
    where page_no = 1
    group by mmxRequestUUID
    having count(*) = 28
),
qtd_sub as (
    select *
    from `etsy-sr-etl-prod.yzhang.qtd_rpc_lqoverlap_boe`
    where mmxRequestUUID in (
        select mmxRequestUUID from subset_requests
    )
),
tmp as (
    select mmxRequestUUID, sum(overlap) as QTD_match, count(*) as total_impression
    from qtd_sub
    where page_no = 1
    and position < 10
    and query_bin = 'top.01'
    -- and array_length(ppath_level2) > 0
    group by mmxRequestUUID
)
-- select avg(tmp.QTD_match)
select count(distinct mmxRequestUUID)
from tmp
