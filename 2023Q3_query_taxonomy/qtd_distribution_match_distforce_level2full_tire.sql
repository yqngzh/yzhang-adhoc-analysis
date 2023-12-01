--------   Level2 full dataset distribution force TIRE result
-- tire results with query and listing taxo data joined
create or replace table `etsy-sr-etl-prod.yzhang.qtd_level2full_tire_dforce_raw` as (
    with tire_output_all as (
        select 
            response.mmxRequestUUID as uuid,
            request.query,
            listing_id, 
            position,
            tireRequestContext.variant as behavior,
            CAST(request.offset / request.limit + 1 AS INTEGER) page_no,
        from `etsy-searchinfra-gke-dev.thrift_tire_listingsv2search_search.rpc_logs_*` as tire_results,
          UNNEST(response.listingIds) AS listing_id  WITH OFFSET position
        where DATE(queryTime) = '2023-11-30'
        and tire_results.request.query != ""
        and tireRequestContext.tireTestv2Id = "ONgqsV7cObQnkCwcirvm"
        and request.limit != 0
    ),
    selected_pages as (
        select distinct uuid
        from tire_output_all
        where page_no = 1 and position < 48
        group by uuid
        having count(distinct listing_id) = 48
    ),
    tire_output as (
        select *
        from tire_output_all
        where page_no = 1 and position < 48
        and uuid in (select uuid from selected_pages)
    )
    select 
        tire_output.*,
        qdata.query_bin, qdata.query_intent, qdata.ppaths, qdata.pcounts,
        ldata.listing_taxo_level2
    from tire_output
    left join `etsy-sr-etl-prod.yzhang.qtd_level2full_tire_query_data` qdata
    on tire_output.query = qdata.query
    left join `etsy-sr-etl-prod.yzhang.qtd_level2full_tire_listing_data` ldata
    on tire_output.listing_id = ldata.listing_id
)


-- dataflow to process data
-- used qtd_distribution_match.py


-- analyze processed data
create or replace table `etsy-sr-etl-prod.yzhang.qtd_level2full_tire_dforce_full` as (
    select r.*, p.qtd_distrib, p.listing_taxo_distrib, p.distrib_distance
    from `etsy-sr-etl-prod.yzhang.qtd_level2full_tire_dforce_raw` r
    join `etsy-sr-etl-prod.yzhang.qtd_level2full_tire_dforce_processed` p
    on r.uuid = p.uuid
    and r.behavior = p.behavior
)

-- sanity check: same total number of rows, number of distinct requests as raw
-- spot check if distribution makes sense
SELECT ppaths, pcounts, qtd_distrib
FROM `etsy-sr-etl-prod.yzhang.qtd_level2full_tire_dforce_full` 
where distrib_distance is not null
-- spot check if distance calculation is as expected
SELECT qtd_distrib, listing_taxo_distrib, distrib_distance
FROM `etsy-sr-etl-prod.yzhang.qtd_level2full_tire_dforce_full` 
where distrib_distance is not null
-- check dist range
SELECT min(distrib_distance), max(distrib_distance)
FROM `etsy-sr-etl-prod.yzhang.qtd_level2full_tire_dforce_full` 
where distrib_distance is not null
-- 0, 2, as expected



--------   Distribution closeness
with tmp as (
    SELECT *
    FROM `etsy-sr-etl-prod.yzhang.qtd_level2full_tire_dforce_full` 
    where array_length(ppaths.list) > 0
    and distrib_distance is not null
    and query_bin = 'top.01'
)
select behavior, 2.0 - avg(distrib_distance) as avg_distrib_closeness
from tmp
group by behavior



--------   Impression match
create or replace table `etsy-sr-etl-prod.yzhang.qtd_level2full_tire_dforce_match` as (
    with fb_data as (
        select `key` as query, ppaths.element as ppaths
        from `etsy-ml-systems-prod.feature_bank_v2.query_feature_bank_2023-11-30`,
            unnest(queryTaxoDemandFeatures_purchaseLevel2TaxonomyPaths.list) as ppaths
    ),
    fb_data_agg as (
        select query, array_agg(ppaths) as ppath_level2
        from fb_data
        group by query
    ),
    qtd_table as (
        select uuid, behavior, query, query_bin, query_intent, listing_taxo_level2
        from `etsy-sr-etl-prod.yzhang.qtd_level2full_tire_dforce_full`
    )
    select qtd_table.*, ppath_level2, if (listing_taxo_level2 in unnest(ppath_level2), 1, 0) as overlap
    from qtd_table
    left join fb_data_agg
    on qtd_table.query = fb_data_agg.query
)
 
with tmp as (
    select uuid, sum(overlap) as QTD_match, count(*) as total_impression
    from `etsy-sr-etl-prod.yzhang.qtd_level2full_tire_dforce_match`
    where array_length(ppath_level2) > 0
    and behavior = 'variant'
    and query_bin = 'top.01'
    group by uuid
)
-- select avg(tmp.QTD_match)
select count(distinct uuid)
from tmp
