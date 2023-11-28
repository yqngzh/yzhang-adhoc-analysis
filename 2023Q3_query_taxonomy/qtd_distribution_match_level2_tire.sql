--------   Level2 boost first page TIRE result
-- query data
create or replace table `etsy-sr-etl-prod.yzhang.qtd_level2_tire_query_data` as (
    with query_fb_data as (
        select 
            `key` as query, queryLevelMetrics_bin as query_bin,
            queryTaxoDemandFeatures_purchaseLevel2TaxonomyPaths as ppaths,
            queryTaxoDemandFeatures_purchaseLevel2TaxonomyCounts as pcounts
        from `etsy-ml-systems-prod.feature_bank_v2.query_feature_bank_2023-11-27`
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

-- listing data
create or replace table `etsy-sr-etl-prod.yzhang.qtd_level2_tire_listing_data` as (
    with fb_data as (
        select 
            `key` as listing_id, 
            split(verticaListings_taxonomyPath, ".")[SAFE_OFFSET(0)] as top_node,
            split(verticaListings_taxonomyPath, ".")[SAFE_OFFSET(1)] as second_node,
        from `etsy-ml-systems-prod.feature_bank_v2.listing_feature_bank_2023-11-27`
    )
    select 
        listing_id,
        if (top_node is not null and second_node is not null, concat(top_node, '.', second_node), null) as listing_taxo_level2
    from fb_data
)

-- tire results with query and listing taxo data joined
create or replace table `etsy-sr-etl-prod.yzhang.qtd_level2_tire_raw` as (
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
        where DATE(queryTime) = '2023-11-27'
        and tire_results.request.query != ""
        and tireRequestContext.tireTestv2Id = "acdQzCVLzfvTlab4mdMl"
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
    left join `etsy-sr-etl-prod.yzhang.qtd_level2_tire_query_data` qdata
    on tire_output.query = qdata.query
    left join `etsy-sr-etl-prod.yzhang.qtd_level2_tire_listing_data` ldata
    on tire_output.listing_id = ldata.listing_id
)


-- dataflow to process data
-- used qtd_distribution_match.py


-- analyze processed data
create or replace table `etsy-sr-etl-prod.yzhang.qtd_level2_tire_full` as (
    select r.*, p.qtd_distrib, p.listing_taxo_distrib, p.distrib_distance
    from `etsy-sr-etl-prod.yzhang.qtd_level2_tire_raw` r
    join `etsy-sr-etl-prod.yzhang.qtd_level2_tire_processed` p
    on r.uuid = p.uuid
    and r.behavior = p.behavior
)

-- sanity check: same total number of rows, number of distinct requests as raw
-- spot check if distribution makes sense
SELECT ppaths, pcounts, qtd_distrib
FROM `etsy-sr-etl-prod.yzhang.qtd_level2_tire_full` 
where distrib_distance is not null
-- spot check if distance calculation is as expected
SELECT qtd_distrib, listing_taxo_distrib, distrib_distance
FROM `etsy-sr-etl-prod.yzhang.qtd_level2_tire_full` 
where distrib_distance is not null
-- check dist range
SELECT min(distrib_distance), max(distrib_distance)
FROM `etsy-sr-etl-prod.yzhang.qtd_level2_tire_full` 
where distrib_distance is not null
-- 0, 2, as expected



--------   Distribution closeness
with tmp as (
    SELECT *
    FROM `etsy-sr-etl-prod.yzhang.qtd_level2_tire_full` 
    where array_length(ppaths.list) > 0
    and distrib_distance is not null
    and query_bin = 'top.01'
)
select behavior, 2.0 - avg(distrib_distance) as avg_distrib_closeness
from tmp
group by behavior



--------   Impression match
create or replace table `etsy-sr-etl-prod.yzhang.qtd_level2_tire_match` as (
    with fb_data as (
        select `key` as query, ppaths.element as ppaths
        from `etsy-ml-systems-prod.feature_bank_v2.query_feature_bank_2023-11-27`,
            unnest(queryTaxoDemandFeatures_purchaseLevel2TaxonomyPaths.list) as ppaths
    ),
    fb_data_agg as (
        select query, array_agg(ppaths) as ppath_level2
        from fb_data
        group by query
    ),
    qtd_table as (
        select uuid, behavior, query, query_bin, query_intent, listing_taxo_level2
        from `etsy-sr-etl-prod.yzhang.qtd_level2_tire_full`
    )
    select qtd_table.*, ppath_level2, if (listing_taxo_level2 in unnest(ppath_level2), 1, 0) as overlap
    from qtd_table
    left join fb_data_agg
    on qtd_table.query = fb_data_agg.query
)
 
with tmp as (
    select uuid, sum(overlap) as QTD_match, count(*) as total_impression
    from `etsy-sr-etl-prod.yzhang.qtd_level2_tire_match`
    where array_length(ppath_level2) > 0
    and behavior = 'variant'
    and query_bin = 'top.01'
    group by uuid
)
-- select avg(tmp.QTD_match)
select count(distinct uuid)
from tmp



--------   Sanity check
-- create temp function getTaxoPaths(path STRING) 
-- returns ARRAY<STRING> 
-- LANGUAGE js AS r""" 
--   if (path == null) {
--     return []
--   }
--   var x = path.split(".")
--   var start = x[0]
--   var result = [start]
--   for (var i=1; i<x.length; i++) {
--     var a = result[result.length-1]
--     result.push(a + "." + x[i])
--   }
--   return result
-- """;

-- CREATE TEMP FUNCTION jsonObjectKeys(input STRING)
-- RETURNS Array<Int64>
-- LANGUAGE js AS """
--   return Object.keys(JSON.parse(input));
-- """;

-- with query_listings as (
--     select jsonObjectKeys(SAFE_CONVERT_BYTES_TO_STRING(FROM_BASE64(q.taxonomies))) as query_taxo_ids,
--         response.mmxRequestUUID as uuid,
--         listing_id, 
--         pos + 1 as position, 
--         div(pos, request.limit) + 1 as page,
--         request.query,
--         tireRequestContext.variant as behavior,
--         request.limit as lmt
--     from `etsy-searchinfra-gke-dev.thrift_tire_listingsv2search_search.rpc_logs_*` as tire_results,
--         unnest(response.listingIds) as listing_id  with offset pos
--     left join `etsy-sr-etl-prod.kbekal_insights.query_taxo_demand_full_id_purchase_level2` as q
--     on tire_results.request.query = q.query
--     WHERE
--         DATE(queryTime) = '2023-11-27'
--         AND q.taxonomies is not null
--         AND tire_results.request.query != ""
--         AND tireRequestContext.tireTestv2Id = "acdQzCVLzfvTlab4mdMl"
--         AND request.limit != 0
-- ),
-- listings_taxonomy_path as (
--   select query_listings.*, getTaxoPaths(listing_fb.verticaListings_taxonomyPath) as taxonomyPathArray 
--   from query_listings
--   LEFT JOIN `etsy-ml-systems-prod.feature_bank_v2.listing_feature_bank_2023-11-27` AS listing_fb
--   ON query_listings.listing_id = listing_fb.key

-- ),
-- listings_taxonomy_path_exploded as (
--   select uuid, listing_id, position, query, behavior, query_taxo_ids, taxopath
--   from listings_taxonomy_path, unnest(taxonomyPathArray) as taxopath
-- ),
-- listings_taxonomy_ids as (
--   select listings_taxonomy_path_exploded.*, taxonomy.taxonomy_id from listings_taxonomy_path_exploded
--   left join `etsy-data-warehouse-prod.structured_data.taxonomy` as taxonomy
--   on listings_taxonomy_path_exploded.taxopath = taxonomy.full_path
-- ),
-- listings_taxonomy_ids_grouped as (
--   select uuid, listing_id, position, query, behavior, array_agg(taxopath IGNORE NULLS) as taxopaths, array_agg(taxonomy_id IGNORE NULLS) as listing_taxo_ids
--   from listings_taxonomy_ids
--   group by uuid, listing_id, position, query, behavior
-- ),
-- listings_taxos_query_taxos_intersect as (
--   select l.*, array(select * from l.listing_taxo_ids intersect distinct (select * from l.query_taxo_ids)) as listing_query_intersect
--   from listings_taxonomy_ids_grouped as l 
-- ),
-- listings_query_intersect_count as (
--   select *, array_length(listing_query_intersect) as intersect_count from listings_taxos_query_taxos_intersect
--   ),
-- listings_query_intersect_count_non_zero as (
--   select * from listings_query_intersect_count
--   where intersect_count > 0
-- ),
-- listings_query_intersect_count_non_zero_grouped as (
--   select uuid, query, behavior, count(*) as non_zero_count
--   from listings_query_intersect_count_non_zero
--   group by uuid, query, behavior
-- )
-- select behavior, avg(non_zero_count) as average
-- from listings_query_intersect_count_non_zero_grouped
-- group by behavior