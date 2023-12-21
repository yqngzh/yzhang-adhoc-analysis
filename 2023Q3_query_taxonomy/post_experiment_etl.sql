-- see post_experiment_analysis.sql for analysis on decision to ramp down

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
-- TODO: has query & listing_id, can join with QTD same as above




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
