-- reference: https://docs.google.com/document/d/1YMK3otWC0f4hxMEoiJ7GbB25DNweQyyE9cJ8oU29LO8/edit#bookmark=id.oh6epkpkuim6

---- visit level impression
create or replace table `etsy-sr-etl-prod.yzhang.qtd_vli_web_1214` as (
    SELECT
        ab.*, 
        views.* EXCEPT(_date, run_date, visit_id)
    FROM `etsy-data-warehouse-prod.catapult.ab_tests` ab
    LEFT JOIN `etsy-data-warehouse-prod.search.visit_level_listing_impressions` views
    ON (
      views.visit_id = ab.visit_id
      AND views._date = ab._date
    )
    WHERE ab._date >= DATE('2023-12-08') AND ab._date <= DATE('2023-12-14')
    AND ab.ab_test = 'ranking/search.mmx.2023_q4.query_taxonomy_demand_try2_web'
    AND views.page = 'search'
)

-- how many visits do we have in each variant where there is at least 1 first page
with tmp as (
  select distinct visit_id, ab_variant 
  from `etsy-sr-etl-prod.yzhang.qtd_vli_web_1214`
  where page_no = 1
)
select ab_variant, count(*)
from tmp 
group by ab_variant
-- 3.7M visits per variant

-- same as above but query have QTD features
with query_taxo as (
    select 
        `key` as query, 
        queryLevelMetrics_bin as query_bin,
        queryTaxoDemandFeatures_purchaseLevel2TaxonomyPaths as ppaths,
        queryTaxoDemandFeatures_purchaseLevel2TaxonomyCounts as pcounts,
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
vli_join_query as (
  select *
  from `etsy-sr-etl-prod.yzhang.qtd_vli_web_1214` vli
  left join query_data qd
  on vli.query = qd.query
  where vli.query != ""
),
v as (
  select distinct visit_id, ab_variant 
  from vli_join_query
  where page_no = 1
  and ppaths is not null
)
select ab_variant, count(*)
from v 
group by ab_variant
-- 2.5M visits



---- from beacon
create or replace table `etsy-sr-etl-prod.yzhang.qtd_beacon_web_1214` as (
  with browsers as (
    select
      split(visit_id, '.')[ORDINAL(1)] as browser_id,
      visit_id,
      ab_variant,
    from `etsy-data-warehouse-prod.catapult.ab_tests`
    where _date between date('2023-12-08') and date('2023-12-14')
    and ab_test = 'ranking/search.mmx.2023_q4.query_taxonomy_demand_try2_web'
    group by 1,2,3
  ),
  searches as (
    select
      b.*,
      (select value from unnest(beacon.properties.key_value) where key = 'query') as query,
      (select value from unnest(beacon.properties.key_value) where key = 'mmx_request_uuid') as mmx_request_uuid,
      listing_id.element as listing_id,
      pos,
    from browsers b
    join `etsy-visit-pipe-prod.canonical.visit_id_beacons` v using (visit_id)
    cross join unnest(beacon.listing_ids.list) listing_id with offset as pos
    where (v._PARTITIONTIME >= TIMESTAMP('2023-12-08') AND v._PARTITIONTIME <= TIMESTAMP('2023-12-14'))
    and beacon.event_name = "search"
  )
)

-- how many visit do we have in each variant
with tmp as (
  select distinct visit_id, ab_variant 
  from `etsy-sr-etl-prod.yzhang.qtd_beacon_web_1214`
)
select ab_variant, count(*)
from tmp 
group by ab_variant
-- 5M visits per variant

-- how many requests do we have in each variant
with tmp as (
  select distinct mmx_request_uuid, ab_variant 
  from `etsy-sr-etl-prod.yzhang.qtd_beacon_web_1214`
)
select ab_variant, count(*)
from tmp 
group by ab_variant
-- 26M request IDs per variant

-- same as above (requests) but with QTD features
with query_taxo as (
    select 
        `key` as query, 
        queryLevelMetrics_bin as query_bin,
        queryTaxoDemandFeatures_purchaseLevel2TaxonomyPaths as ppaths,
        queryTaxoDemandFeatures_purchaseLevel2TaxonomyCounts as pcounts,
    from `etsy-ml-systems-prod.feature_bank_v2.query_feature_bank_2023-12-14`
    where queryTaxoDemandFeatures_purchaseLevel2TaxonomyPaths is not null
    and array_length(queryTaxoDemandFeatures_purchaseLevel2TaxonomyPaths.list) > 0
    and queryTaxoDemandFeatures_purchaseLevel2TaxonomyPaths.list[0].element != ''
    and queryTaxoDemandFeatures_purchaseCount >= 5
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
  select alb.listing_id, taxo.full_path 
  from `etsy-data-warehouse-prod.rollups.active_listing_basics` alb
  join `etsy-data-warehouse-prod.structured_data.taxonomy` taxo
  on alb.taxonomy_id = taxo.taxonomy_id
),
beacon_join_query as (
  select *
  from `etsy-sr-etl-prod.yzhang.qtd_beacon_web_1214` beacon
  left join query_data qd
  on beacon.query = qd.query
  left join listing_data ld
  on beacon.listing_id = ld.listing_id
  where beacon.query != ""
),
requests as (
  select distinct mmx_request_uuid, query_bin, ab_variant 
  from beacon_join_query
  where ppaths is not null
  and full_path is not null
)
select ab_variant, query_bin, count(*) as number_mmx_request_uuid
from requests
group by ab_variant, query_bin
order by ab_variant, query_bin

