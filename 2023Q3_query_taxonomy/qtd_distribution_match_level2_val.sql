-------- QTD level 2
-- step 1. query data
create or replace table `etsy-sr-etl-prod.yzhang.qtd_distrib_match_query_data` as (
    with query_fb_data as (
        select 
            `key` as query, queryLevelMetrics_bin as query_bin,
            queryTaxoDemandFeatures_purchaseLevel2TaxonomyPaths as ppaths,
            queryTaxoDemandFeatures_purchaseLevel2TaxonomyCounts as pcounts
        from `etsy-ml-systems-prod.feature_bank_v2.query_feature_bank_2023-11-09`
        where queryTaxoDemandFeatures_purchaseLevel2TaxonomyPaths is not null
    ),
    query_intent_data as (
        select query_raw, inference.label as query_intent
        from `etsy-data-warehouse-prod.arizona.query_intent_labels`
        QUALIFY ROW_NUMBER() OVER(PARTITION BY query_raw ORDER BY inference.confidence DESC) = 1
    )
    select fb.*, qi.query_intent
    from query_fb_data fb
    left join query_intent_data qi
    on fb.query = qi.query_raw
)
-- sanity check
-- select count(*)
-- from `etsy-ml-systems-prod.feature_bank_v2.query_feature_bank_2023-11-09`
-- where queryTaxoDemandFeatures_purchaseLevel2TaxonomyPaths is not null
-- -- same as in qtd_distrib_match_query_data


-- step 2. listing data
create or replace table `etsy-sr-etl-prod.yzhang.qtd_distrib_match_listing_data` as (
    with fb_data as (
        select 
            `key` as listing_id, 
            split(verticaListings_taxonomyPath, ".")[SAFE_OFFSET(0)] as top_node,
            split(verticaListings_taxonomyPath, ".")[SAFE_OFFSET(1)] as second_node,
        from `etsy-ml-systems-prod.feature_bank_v2.listing_feature_bank_2023-11-09`
    )
    select 
        listing_id,
        if (top_node is not null and second_node is not null, concat(top_node, '.', second_node), null) as listing_taxo_level2
    from fb_data
)
-- sanity check: same number of listings in qtd and in listing feature bank


-- step 3. query level 2 raw data for computing distribution dist
create or replace table `etsy-sr-etl-prod.yzhang.qtd_distrib_match_qtdlevel2_raw` as (
    select 
        boost.*,
        qdata.query_bin, qdata.query_intent, qdata.ppaths, qdata.pcounts,
        ldata.listing_taxo_level2
    from `etsy-sr-etl-prod.kbekal_insights.qtd_boosting_level2` boost
    left join `etsy-sr-etl-prod.yzhang.qtd_distrib_match_query_data` qdata
    on boost.query = qdata.query
    left join `etsy-sr-etl-prod.yzhang.qtd_distrib_match_listing_data` ldata
    on boost.listing_id = ldata.listing_id
)

-- sanity check: each left join maintains the number of rows
with unique_request as (
    select distinct uuid, behavior
    from `etsy-sr-etl-prod.yzhang.qtd_distrib_match_qtdlevel2_raw`
    where ppaths is not null
    and array_length(ppaths.list) > 0
)
select count(*) from unique_request
-- how many distinct request are in dataset (uuid, behavior)
-- -- 20352, half = 10176 control
-- how many requests have have QTD feature
-- -- 6526


-- step 4. dataflow => qtd_distrib_match_qtdlevel2_processed
-- sanity check 
-- 20352 requests in table
-- 6440 requests with distribution distance, control & variant half and half


-- step 5. analyze processed data
create or replace table `etsy-sr-etl-prod.yzhang.qtd_distrib_match_qtdlevel2_full` as (
    select r.*, p.qtd_distrib, p.listing_taxo_distrib, p.distrib_distance
    from `etsy-sr-etl-prod.yzhang.qtd_distrib_match_qtdlevel2_raw` r
    join `etsy-sr-etl-prod.yzhang.qtd_distrib_match_qtdlevel2_processed` p
    on r.uuid = p.uuid
    and r.behavior = p.behavior
)

-- sanity check: same total number of rows, number of distinct requests as raw
-- spot check if distribution makes sense
SELECT ppaths, pcounts, qtd_distrib
FROM `etsy-sr-etl-prod.yzhang.qtd_distrib_match_qtdlevel2_full` 
where distrib_distance is not null
-- spot check if distance calculation is as expected
SELECT qtd_distrib, listing_taxo_distrib, distrib_distance
FROM `etsy-sr-etl-prod.yzhang.qtd_distrib_match_qtdlevel2_full` 
where distrib_distance is not null
-- check dist range
SELECT min(distrib_distance), max(distrib_distance)
FROM `etsy-sr-etl-prod.yzhang.qtd_distrib_match_qtdlevel2_full` 
where distrib_distance is not null
-- 0, 2, as expected

-- average distribution distance across requests by variant
with tmp as (
    SELECT *
    FROM `etsy-sr-etl-prod.yzhang.qtd_distrib_match_qtdlevel2_full` 
    where distrib_distance is not null
)
select behavior, 2.0 - avg(distrib_distance) as avg_distrib_closeness
from tmp
group by behavior

with tmp as (
    SELECT *
    FROM `etsy-sr-etl-prod.yzhang.qtd_distrib_match_qtdlevel2_full` 
    where distrib_distance is not null
    and query_bin = 'top.01'
)
select behavior, 2.0 - avg(distrib_distance) as avg_distrib_closeness
from tmp
group by behavior


---- Impression overlap
create or replace table `etsy-sr-etl-prod.yzhang.qtd_boosting_level2_val` as (
    with fb_data as (
        select `key` as query, ppaths.element as ppaths
        from `etsy-ml-systems-prod.feature_bank_v2.query_feature_bank_2023-11-09`,
            unnest(queryTaxoDemandFeatures_purchaseLevel2TaxonomyPaths.list) as ppaths
    ),
    fb_data_agg as (
        select query, array_agg(ppaths) as ppath_level2
        from fb_data
        group by query
    ),
    qtd_table as (
        select uuid, behavior, query, query_bin, query_intent, listing_taxo_level2
        from `etsy-sr-etl-prod.yzhang.qtd_distrib_match_qtdlevel2_full`
    )
    select qtd_table.*, ppath_level2, if (listing_taxo_level2 in unnest(ppath_level2), 1, 0) as overlap
    from qtd_table
    left join fb_data_agg
    on qtd_table.query = fb_data_agg.query
) 

with tmp as (
    select uuid, sum(overlap) as QTD_match, count(*) as total_impression
    from `etsy-sr-etl-prod.yzhang.qtd_boosting_level2_val`
    where array_length(ppath_level2) > 0
    and behavior = 'control'
    -- and query_bin = 'top.01'
    group by uuid
)
select avg(QTD_match)
from tmp
