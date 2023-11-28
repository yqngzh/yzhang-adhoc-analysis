---- Last A/B test data
-- step 1. query data
create or replace table `etsy-sr-etl-prod.yzhang.qtd_distrib_match_lastab_qdata` as (
    with query_taxo as (
        select 
            `key` as query, queryLevelMetrics_bin as query_bin,
            queryTaxoDemandFeatures_purchaseLevel2TaxonomyPaths as ppaths,
            queryTaxoDemandFeatures_purchaseLevel2TaxonomyCounts as pcounts,
        from `etsy-ml-systems-prod.feature_bank_v2.query_feature_bank_2023-11-09` 
        where queryTaxoDemandFeatures_purchaseLevel2TaxonomyPaths is not null
    ),
    query_intent_data as (
        select query_raw, inference.label as query_intent
        from `etsy-data-warehouse-prod.arizona.query_intent_labels`
        QUALIFY ROW_NUMBER() OVER(PARTITION BY query_raw ORDER BY inference.confidence DESC) = 1
    )
    select qt.*, qi.query_intent
    from query_taxo qt
    left join query_intent_data qi
    on qt.query = qi.query_raw
)
-- 06-12 query feature bank no longer available, using the same as the rest of datasets


-- step 2. last AB test raw data for computing distribution dist
-- separate each page to get page-wise distribution
-- considered canonical.visit_id_beacons
-- but the visits from experiment no longer exists in the table
-- instead, previously generated table has page_no; do an ugly estimate
create or replace table `etsy-sr-etl-prod.yzhang.qtd_distrib_match_lastab_raw` as (
    with taxo_data as (
        select taxonomy_id, 
            split(full_path, ".")[SAFE_OFFSET(0)] as top_node,
            split(full_path, ".")[SAFE_OFFSET(1)] as second_node,
        from `etsy-data-warehouse-prod.structured_data.taxonomy`
    ),
    taxo_data_level2 as (
        select taxonomy_id,
            if (top_node is not null and second_node is not null, concat(top_node, '.', second_node), null) as listing_taxo_level2
        from taxo_data
    ),
    raw_data_full as (
        select
            visit_id, ab_web.query, ab_variant as behavior, query_bin, query_intent,
            ppaths, pcounts, listing_taxo_level2,
            impressions, clicks, purchases, price_usd, user_id, page_no, listing_id
        from `etsy-sr-etl-prod.search_ab_tests.taxo_demand_web_0612` as ab_web
        left join `etsy-sr-etl-prod.yzhang.qtd_distrib_match_lastab_qdata` qdata
        on ab_web.query = qdata.query
        left join taxo_data_level2 taxo
        on ab_web.taxonomy_id = taxo.taxonomy_id
    ),
    selected_pages as (
        select 
            visit_id, query, behavior, page_no, 
            concat(visit_id, query, behavior, cast(page_no as string)) as uuid
        from raw_data_full
        group by visit_id, query, behavior, page_no
        having page_no = 1
        and count(distinct listing_id) = 48
    )
    select sp.uuid, raw_data_full.*
    from raw_data_full
    join selected_pages sp
    on raw_data_full.visit_id = sp.visit_id
    and raw_data_full.query = sp.query 
    and raw_data_full.behavior = sp.behavior 
    and raw_data_full.page_no = sp.page_no 
)
-- uuid estimating requests, from the same visit, variant, query, page_no = 1 (first page) & have 48 listings
-- sanity check
-- select behavior, count(distinct uuid)
-- from `etsy-sr-etl-prod.yzhang.qtd_distrib_match_lastab_raw`
-- group by behavior
-- same as in selected_pages when running only raw_data_full + selected_pages
-- split by query bin, do number of uuids add up? yes


-- step 3. dataflow to process data
-- used qtd_distribution_match.py


-- step 4. analyze processed data
create or replace table `etsy-sr-etl-prod.yzhang.qtd_distrib_match_lastab_full` as (
    select r.*, p.qtd_distrib, p.listing_taxo_distrib, p.distrib_distance
    from `etsy-sr-etl-prod.yzhang.qtd_distrib_match_lastab_raw` r
    join `etsy-sr-etl-prod.yzhang.qtd_distrib_match_lastab_processed` p
    on r.uuid = p.uuid
    and r.behavior = p.behavior
)

-- sanity check: same total number of rows, number of distinct requests as raw
-- spot check if distribution makes sense
SELECT ppaths, pcounts, qtd_distrib
FROM `etsy-sr-etl-prod.yzhang.qtd_distrib_match_lastab_full` 
where distrib_distance is not null
-- spot check if distance calculation is as expected
SELECT qtd_distrib, listing_taxo_distrib, distrib_distance
FROM `etsy-sr-etl-prod.yzhang.qtd_distrib_match_lastab_full` 
where distrib_distance is not null
-- check dist range
SELECT min(distrib_distance), max(distrib_distance)
FROM `etsy-sr-etl-prod.yzhang.qtd_distrib_match_lastab_full` 
where distrib_distance is not null
-- 0, 2, as expected

-- average distribution distance across requests by variant
with tmp as (
    SELECT *
    FROM `etsy-sr-etl-prod.yzhang.qtd_distrib_match_lastab_full` 
    where array_length(ppaths.list) > 0
    and distrib_distance is not null
    and query_bin = 'top.01'
)
select behavior, 2.0 - avg(distrib_distance) as avg_distrib_closeness
from tmp
group by behavior



---- Last A/B test data: impression match
create or replace table `etsy-sr-etl-prod.yzhang.qtd_boosting_lastab` as (
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
        from `etsy-sr-etl-prod.yzhang.qtd_distrib_match_lastab_full`
    )
    select qtd_table.*, ppath_level2, if (listing_taxo_level2 in unnest(ppath_level2), 1, 0) as overlap
    from qtd_table
    left join fb_data_agg
    on qtd_table.query = fb_data_agg.query
)
 
with tmp as (
    select uuid, sum(overlap) as QTD_match, count(*) as total_impression
    from `etsy-sr-etl-prod.yzhang.qtd_boosting_lastab`
    where array_length(ppath_level2) > 0
    and behavior = 'off'
    -- and query_bin = 'top.01'
    group by uuid
)
-- select avg(tmp.QTD_match)
select count(distinct uuid)
from tmp
