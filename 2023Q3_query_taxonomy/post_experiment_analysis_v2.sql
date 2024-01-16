-- see post_experiment_analysis_v1.sql for analysis on decision to ramp down

------ Web
-- join catapult.ab_tests with canonical.visit_id_beacons to get events/requests
-- source: https://etsy.slack.com/archives/C0651JPPF0V/p1700158752951709?thread_ts=1700019435.264389&cid=C0651JPPF0V
create or replace table `etsy-sr-etl-prod.yzhang.qtd_ab_web_beacon` as (
  with ab_data as (
    select
      split(visit_id, '.')[OFFSET(0)] as browser_id,
      visit_id,
      ab_variant,
    from `etsy-data-warehouse-prod.catapult.ab_tests`
    where _date between date('2023-12-08') and date('2023-12-14')
    and ab_test = 'ranking/search.mmx.2023_q4.query_taxonomy_demand_try2_web'
    group by 1,2,3
  ),
  visit_beacon_data as (
    select
      visit_id,
      (select value from unnest(beacon.properties.key_value) where key = 'query') as query,
      (select value from unnest(beacon.properties.key_value) where key = 'mmx_request_uuid') as mmx_request_uuid,
      listing_id.element as listing_id,
      position,
    from `etsy-visit-pipe-prod.canonical.visit_id_beacons`,
      unnest(beacon.listing_ids.list) listing_id with offset as position
    where (_PARTITIONTIME >= TIMESTAMP('2023-12-08') AND _PARTITIONTIME <= TIMESTAMP('2023-12-14'))
    and beacon.event_name = "search"
  )
  select ab.*, vb.* except(visit_id)
  from ab_data ab
  join visit_beacon_data vb
  on ab.visit_id = vb.visit_id
)

-- further join with event attribution for event-level metrics
-- source: https://github.etsycorp.com/Engineering/BigData/blob/858486ad96f984884b8350014dccb473665513e3/sql/src/search-online-metrics/compute_event_attribution_join.sql
create or replace table `etsy-sr-etl-prod.yzhang.qtd_ab_web_event_attr` as (
  select 
    qtd.*, 
    attrs.userId,
    attrs.attributions
  from `etsy-sr-etl-prod.yzhang.qtd_ab_web_beacon` qtd
  join `etsy-sr-etl-prod.etl_data.search_attribution_v2` attrs
  on qtd.mmx_request_uuid = attrs.requestUUID
  and qtd.visit_id = attrs.visitId
  and qtd.listing_id = attrs.candidateId
  and qtd.position = attrs.candidatePosition
  where attrs.userCountry = "US"
)

-- event-level join with QTD and listing taxonomy
create or replace table `etsy-sr-etl-prod.yzhang.qtd_ab_web_event_merged` as (
  with query_taxo as (
      select 
          `key` as query, 
          queryLevelMetrics_bin as query_bin,
          queryTaxoDemandFeatures_purchaseLevel2TaxonomyPaths as qtd_purchase_paths,
          queryTaxoDemandFeatures_purchaseLevel2TaxonomyCounts as qtd_purchase_counts,
      from `etsy-ml-systems-prod.feature_bank_v2.query_feature_bank_2023-12-14`
      where queryTaxoDemandFeatures_purchaseLevel2TaxonomyPaths is not null
      and array_length(queryTaxoDemandFeatures_purchaseLevel2TaxonomyPaths.list) > 0
      and queryTaxoDemandFeatures_purchaseLevel2TaxonomyPaths.list[0].element != ''
  ),
  query_intent_data as (
      select query_raw, inference.label as query_intent
      from `etsy-data-warehouse-prod.arizona.query_intent_labels`
      QUALIFY ROW_NUMBER() OVER(PARTITION BY query_raw ORDER BY inference.confidence DESC) = 1
  ),
  query_data as (
    select qt.*, qi.query_intent
    from query_taxo qt
    left join query_intent_data qi
    on qt.query = qi.query_raw
  ),
  listing_data as (
    select alb.listing_id, taxo.full_path as listing_taxo_full_path
    from `etsy-data-warehouse-prod.rollups.active_listing_basics` alb
    join `etsy-data-warehouse-prod.structured_data.taxonomy` taxo
    on alb.taxonomy_id = taxo.taxonomy_id
  ),
  user_data as (
    select `key` as user_id, userSegmentFeatures_buyerSegment as buyer_segment
    from `etsy-ml-systems-prod.feature_bank_v2.user_feature_bank_2023-12-14`
  )
  select 
    qtd.*, 
    qd.query_bin, qd.query_intent, qd.qtd_purchase_paths, qd.qtd_purchase_counts,
    ld.listing_taxo_full_path,
    if (qtd.userId = 0, "Signed out", ud.buyer_segment) as buyer_segment
  from `etsy-sr-etl-prod.yzhang.qtd_ab_web_event_attr` qtd
  left join query_data qd
  on qtd.query = qd.query
  left join listing_data ld
  on qtd.listing_id = ld.listing_id
  left join user_data ud
  on qtd.userId = ud.user_id
)


-- visit-level listing metrics
create or replace table `etsy-sr-etl-prod.yzhang.qtd_ab_web_visit_metrics` as (
  select
    split(ab.visit_id, '.')[OFFSET(0)] as browser_id,
    ab.visit_id,
    ab.ab_variant,
    views.* except(_date, run_date, visit_id)
  from `etsy-data-warehouse-prod.catapult.ab_tests` ab
  left join `etsy-data-warehouse-prod.search.visit_level_listing_impressions` views
  on (
    views.visit_id = ab.visit_id
    and views._date = ab._date
  )
  where ab._date between date('2023-12-08') and date('2023-12-14')
  and ab.ab_test = 'ranking/search.mmx.2023_q4.query_taxonomy_demand_try2_web'
  and views.page = 'search'
)
-- has query & listing_id, can join with QTD same as above


-- get page number from RPC logs, filter to 1st pages with 48 listings
create or replace table `etsy-sr-etl-prod.yzhang.qtd_ab_web_raw` as (
  with rpc_data as (
    SELECT
        response.mmxRequestUUID,
        request.query,
        CAST(request.offset / request.limit + 1 AS INTEGER) page_no,
        listingId,
    FROM `etsy-searchinfra-gke-prod-2.thrift_mmx_listingsv2search_search.rpc_logs_*`,
      UNNEST(response.listingIds) AS listingId
    WHERE request.options.searchPlacement = "wsg"
    AND DATE(queryTime) between DATE('2023-12-08') and date('2023-12-14')
    AND request.options.csrOrganic = TRUE
    AND (request.offset + request.limit) < 144
    AND request.options.mmxBehavior.matching IS NOT NULL
    AND request.options.mmxBehavior.ranking IS NOT NULL
    AND request.options.mmxBehavior.marketOptimization IS NOT NULL
  ),
  event_listing_level2_taxo as (
    select 
      *,
      split(listing_taxo_full_path, ".")[SAFE_OFFSET(0)] as top_node,
      split(listing_taxo_full_path, ".")[SAFE_OFFSET(1)] as second_node,
    from `etsy-sr-etl-prod.yzhang.qtd_ab_web_event_merged`
  ),
  first_page_events as (
    select e.*, rpc_data.page_no
    from event_listing_level2_taxo e
    left join rpc_data
    on (
      e.mmx_request_uuid = rpc_data.mmxRequestUUID
      and e.query = rpc_data.query
      and e.listing_id = rpc_data.listingId
    )
    where rpc_data.page_no = 1
  ),
  request_num_listings as (
    select mmx_request_uuid, ab_variant, query
    from first_page_events
    group by mmx_request_uuid, ab_variant, query
    having count(listing_id) = 48
  )
  select 
    fpe.mmx_request_uuid as uuid,
    fpe.query,
    fpe.listing_id,
    fpe.position,
    fpe.ab_variant as behavior,
    fpe.query_bin, 
    fpe.query_intent, 
    fpe.qtd_purchase_paths as ppaths,
    fpe.qtd_purchase_counts as pcounts,
    if (fpe.top_node is not null and fpe.second_node is not null, concat(fpe.top_node, '.', fpe.second_node), null) as listing_taxo_level2
  from first_page_events fpe
  where fpe.mmx_request_uuid in (
    select distinct rnl.mmx_request_uuid from request_num_listings rnl
  )
)

-- (not used) get page number from visit level listing impression
-- create or replace table `etsy-sr-etl-prod.yzhang.qtd_ab_web_raw` as (
--   select e.*, v.page_no
--   from `etsy-sr-etl-prod.yzhang.qtd_ab_web_event_merged` e
--   left join `etsy-sr-etl-prod.yzhang.qtd_ab_web_visit_metrics` v
--   on (
--     e.browser_id = v.browser_id
--     and e.visit_id = v.visit_id
--     and e.ab_variant = v.ab_variant
--     and e.listing_id = v.listing_id
--     and e.query = v.query
--   )
-- )

-- sanity check
with unique_request as (
    select distinct uuid, behavior
    from `etsy-sr-etl-prod.yzhang.qtd_ab_web_raw`
    where ppaths is not null
    and array_length(ppaths.list) > 0
)
select behavior, count(*) as n_requests
from unique_request
group by behavior

-- dataflow: post_experiment_dataflow.py

-- analyze processed data
create or replace table `etsy-sr-etl-prod.yzhang.qtd_ab_web_full` as (
    select r.*, p.qtd_distrib, p.listing_taxo_distrib, p.distrib_distance
    from `etsy-sr-etl-prod.yzhang.qtd_ab_web_raw` r
    join `etsy-sr-etl-prod.yzhang.qtd_ab_web_processed` p
    on r.uuid = p.uuid
    and r.behavior = p.behavior
)

-- sanity check: same total number of rows, number of distinct requests as raw
-- spot check if distribution makes sense
SELECT ppaths, pcounts, qtd_distrib
FROM `etsy-sr-etl-prod.yzhang.qtd_ab_web_full` 
where distrib_distance is not null
-- spot check if distance calculation is as expected
SELECT qtd_distrib, listing_taxo_distrib, distrib_distance
FROM `etsy-sr-etl-prod.yzhang.qtd_ab_web_full` 
where distrib_distance is not null
-- check dist range
SELECT min(distrib_distance), max(distrib_distance)
FROM `etsy-sr-etl-prod.yzhang.qtd_ab_web_full` 
where distrib_distance is not null
-- 0, 2, as expected

-- distribution closeness
with tmp as (
    SELECT *
    FROM `etsy-sr-etl-prod.yzhang.qtd_ab_web_full` 
    where array_length(ppaths.list) > 0
    and distrib_distance is not null
    and query_bin = 'top.01'
)
select behavior, 2.0 - avg(distrib_distance) as avg_distrib_closeness
from tmp
group by behavior

-- impression match
create or replace table `etsy-sr-etl-prod.yzhang.qtd_ab_web_match` as (
    with qtd_data as (
        select distinct query, ppaths.element as ppaths
        from `etsy-sr-etl-prod.yzhang.qtd_ab_web_full`,
            unnest(ppaths.list) as ppaths
    ),
    qtd_data_agg as (
        select query, array_agg(ppaths) as ppath_level2
        from qtd_data
        group by query
    ),
    qtd_table as (
        select uuid, behavior, query, query_bin, query_intent, listing_taxo_level2
        from `etsy-sr-etl-prod.yzhang.qtd_ab_web_full`
    )
    select qtd_table.*, ppath_level2, if (listing_taxo_level2 in unnest(ppath_level2), 1, 0) as overlap
    from qtd_table
    left join qtd_data_agg
    on qtd_table.query = qtd_data_agg.query
)
 
with tmp as (
    select uuid, sum(overlap) as QTD_match, count(*) as total_impression
    from `etsy-sr-etl-prod.yzhang.qtd_ab_web_match`
    where ppath_level2 is not null
    and array_length(ppath_level2) > 0
    and behavior = 'qtd_force_dist_level2_web'
    and query_bin = 'top.01'
    group by uuid
)
select avg(tmp.QTD_match)
-- select count(distinct uuid)
from tmp




------ BOE
create or replace table `etsy-sr-etl-prod.yzhang.qtd_ab_boe_beacon` as (
  with ab_data as (
    select
      split(visit_id, '.')[OFFSET(0)] as browser_id,
      visit_id,
      ab_variant,
    from `etsy-data-warehouse-prod.catapult.ab_tests`
    where _date between date('2023-12-08') and date('2023-12-14')
    and ab_test = 'ranking/search.mmx.2023_q4.query_taxonomy_demand_try2_boe'
    group by 1,2,3
  ),
  visit_beacon_data as (
    select
      visit_id,
      (select value from unnest(beacon.properties.key_value) where key = 'query') as query,
      (select value from unnest(beacon.properties.key_value) where key = 'mmx_request_uuid') as mmx_request_uuid,
      listing_id.element as listing_id,
      position,
    from `etsy-visit-pipe-prod.canonical.visit_id_beacons`,
      unnest(beacon.listing_ids.list) listing_id with offset as position
    where (_PARTITIONTIME >= TIMESTAMP('2023-12-08') AND _PARTITIONTIME <= TIMESTAMP('2023-12-14'))
    and beacon.event_name = "search"
  )
  select ab.*, vb.* except(visit_id)
  from ab_data ab
  join visit_beacon_data vb
  on ab.visit_id = vb.visit_id
)

create or replace table `etsy-sr-etl-prod.yzhang.qtd_ab_boe_event_attr` as (
  select 
    qtd.*, 
    attrs.userId,
    attrs.attributions
  from `etsy-sr-etl-prod.yzhang.qtd_ab_boe_beacon` qtd
  join `etsy-sr-etl-prod.etl_data.search_attribution_v2_boe` attrs
  on qtd.mmx_request_uuid = attrs.requestUUID
  and qtd.visit_id = attrs.visitId
  and qtd.listing_id = attrs.candidateId
  and qtd.position = attrs.candidatePosition
  where attrs.userCountry = "US"
)

create or replace table `etsy-sr-etl-prod.yzhang.qtd_ab_boe_event_merged` as (
  with query_taxo as (
      select 
          `key` as query, 
          queryLevelMetrics_bin as query_bin,
          queryTaxoDemandFeatures_purchaseLevel2TaxonomyPaths as qtd_purchase_paths,
          queryTaxoDemandFeatures_purchaseLevel2TaxonomyCounts as qtd_purchase_counts,
      from `etsy-ml-systems-prod.feature_bank_v2.query_feature_bank_2023-12-14`
      where queryTaxoDemandFeatures_purchaseLevel2TaxonomyPaths is not null
      and array_length(queryTaxoDemandFeatures_purchaseLevel2TaxonomyPaths.list) > 0
      and queryTaxoDemandFeatures_purchaseLevel2TaxonomyPaths.list[0].element != ''
  ),
  query_intent_data as (
      select query_raw, inference.label as query_intent
      from `etsy-data-warehouse-prod.arizona.query_intent_labels`
      QUALIFY ROW_NUMBER() OVER(PARTITION BY query_raw ORDER BY inference.confidence DESC) = 1
  ),
  query_data as (
    select qt.*, qi.query_intent
    from query_taxo qt
    left join query_intent_data qi
    on qt.query = qi.query_raw
  ),
  listing_data as (
    select alb.listing_id, taxo.full_path as listing_taxo_full_path
    from `etsy-data-warehouse-prod.rollups.active_listing_basics` alb
    join `etsy-data-warehouse-prod.structured_data.taxonomy` taxo
    on alb.taxonomy_id = taxo.taxonomy_id
  ),
  user_data as (
    select `key` as user_id, userSegmentFeatures_buyerSegment as buyer_segment
    from `etsy-ml-systems-prod.feature_bank_v2.user_feature_bank_2023-12-14`
  )
  select 
    qtd.*, 
    qd.query_bin, qd.query_intent, qd.qtd_purchase_paths, qd.qtd_purchase_counts,
    ld.listing_taxo_full_path,
    if (qtd.userId = 0, "Signed out", ud.buyer_segment) as buyer_segment
  from `etsy-sr-etl-prod.yzhang.qtd_ab_boe_event_attr` qtd
  left join query_data qd
  on qtd.query = qd.query
  left join listing_data ld
  on qtd.listing_id = ld.listing_id
  left join user_data ud
  on qtd.userId = ud.user_id
)

create or replace table `etsy-sr-etl-prod.yzhang.qtd_ab_boe_visit_metrics` as (
  select
    split(ab.visit_id, '.')[OFFSET(0)] as browser_id,
    ab.visit_id,
    ab.ab_variant,
    views.* except(_date, run_date, visit_id)
  from `etsy-data-warehouse-prod.catapult.ab_tests` ab
  left join `etsy-data-warehouse-prod.search.visit_level_listing_impressions` views
  on (
    views.visit_id = ab.visit_id
    and views._date = ab._date
  )
  where ab._date between date('2023-12-08') and date('2023-12-14')
  and ab.ab_test = 'ranking/search.mmx.2023_q4.query_taxonomy_demand_try2_boe'
  and views.page = 'search'
)

create or replace table `etsy-sr-etl-prod.yzhang.qtd_ab_boe_raw` as (
  with rpc_data as (
    SELECT
        response.mmxRequestUUID,
        request.query,
        CAST(request.offset / request.limit + 1 AS INTEGER) page_no,
        listingId,
    FROM `etsy-searchinfra-gke-prod-2.thrift_mmx_listingsv2search_search.rpc_logs_*`,
      UNNEST(response.listingIds) AS listingId
    WHERE request.options.searchPlacement = "allsr"
    AND DATE(queryTime) between DATE('2023-12-08') and date('2023-12-14')
    AND request.options.csrOrganic = TRUE
    AND (request.offset + request.limit) < 84
    AND request.options.mmxBehavior.matching IS NOT NULL
    AND request.options.mmxBehavior.ranking IS NOT NULL
    AND request.options.mmxBehavior.marketOptimization IS NOT NULL
  ),
  event_listing_level2_taxo as (
    select 
      *,
      split(listing_taxo_full_path, ".")[SAFE_OFFSET(0)] as top_node,
      split(listing_taxo_full_path, ".")[SAFE_OFFSET(1)] as second_node,
    from `etsy-sr-etl-prod.yzhang.qtd_ab_boe_event_merged`
  ),
  first_page_events as (
    select e.*, rpc_data.page_no
    from event_listing_level2_taxo e
    left join rpc_data
    on (
      e.mmx_request_uuid = rpc_data.mmxRequestUUID
      and e.query = rpc_data.query
      and e.listing_id = rpc_data.listingId
    )
    where rpc_data.page_no = 1
  ),
  request_num_listings as (
    select mmx_request_uuid, ab_variant, query
    from first_page_events
    group by mmx_request_uuid, ab_variant, query
    having count(listing_id) = 28
  )
  select 
    fpe.mmx_request_uuid as uuid,
    fpe.query,
    fpe.listing_id,
    fpe.position,
    fpe.ab_variant as behavior,
    fpe.query_bin, 
    fpe.query_intent, 
    fpe.qtd_purchase_paths as ppaths,
    fpe.qtd_purchase_counts as pcounts,
    if (fpe.top_node is not null and fpe.second_node is not null, concat(fpe.top_node, '.', fpe.second_node), null) as listing_taxo_level2
  from first_page_events fpe
  where fpe.mmx_request_uuid in (
    select distinct rnl.mmx_request_uuid from request_num_listings rnl
  )
)

-- sanity check
with unique_request as (
    select distinct uuid, behavior
    from `etsy-sr-etl-prod.yzhang.qtd_ab_boe_raw`
    where ppaths is not null
    and array_length(ppaths.list) > 0
)
select behavior, count(*) as n_requests
from unique_request
group by behavior

-- dataflow

-- analyze processed data
create or replace table `etsy-sr-etl-prod.yzhang.qtd_ab_boe_full` as (
    select r.*, p.qtd_distrib, p.listing_taxo_distrib, p.distrib_distance
    from `etsy-sr-etl-prod.yzhang.qtd_ab_boe_raw` r
    join `etsy-sr-etl-prod.yzhang.qtd_ab_boe_processed` p
    on r.uuid = p.uuid
    and r.behavior = p.behavior
)

-- distribution closeness
with tmp as (
    SELECT *
    FROM `etsy-sr-etl-prod.yzhang.qtd_ab_boe_full` 
    where array_length(ppaths.list) > 0
    and distrib_distance is not null
    and query_bin = 'top.01'
)
select behavior, 2.0 - avg(distrib_distance) as avg_distrib_closeness
from tmp
group by behavior

-- impression match
create or replace table `etsy-sr-etl-prod.yzhang.qtd_ab_boe_match` as (
    with qtd_data as (
        select distinct query, ppaths.element as ppaths
        from `etsy-sr-etl-prod.yzhang.qtd_ab_boe_full`,
            unnest(ppaths.list) as ppaths
    ),
    qtd_data_agg as (
        select query, array_agg(ppaths) as ppath_level2
        from qtd_data
        group by query
    ),
    qtd_table as (
        select uuid, behavior, query, query_bin, query_intent, listing_taxo_level2
        from `etsy-sr-etl-prod.yzhang.qtd_ab_boe_full`
    )
    select qtd_table.*, ppath_level2, if (listing_taxo_level2 in unnest(ppath_level2), 1, 0) as overlap
    from qtd_table
    left join qtd_data_agg
    on qtd_table.query = qtd_data_agg.query
)
 
with tmp as (
    select uuid, sum(overlap) as QTD_match, count(*) as total_impression
    from `etsy-sr-etl-prod.yzhang.qtd_ab_boe_match`
    where ppath_level2 is not null
    and array_length(ppath_level2) > 0
    and behavior = 'qtd_force_dist_level2_web'
    and query_bin = 'top.01'
    group by uuid
)
select avg(tmp.QTD_match)
-- select count(distinct uuid)
from tmp
