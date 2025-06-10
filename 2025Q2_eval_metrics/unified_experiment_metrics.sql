DECLARE start_date DATE DEFAULT "2025-05-23";
DECLARE end_date DATE DEFAULT "2025-06-04";

DECLARE experiment_name STRING DEFAULT "ranking/search.mmx.2025_q2.nrv2_unified_ranking_try2";
DECLARE is_filtered BOOLEAN DEFAULT FALSE;
DECLARE bucketing_id_type INT64 DEFAULT 1;


----  Online
CREATE OR REPLACE TABLE `etsy-search-ml-dev.yzhang.online_pndcg` AS (
    SELECT
        _date,
        boundary_start_ts,
        variant_id,
        bucketing_id,
        bucketing_ts,
        event_id,
        event_value,
    FROM `etsy-data-warehouse-prod.catapult_unified.aggregated_event_daily`
    WHERE _date BETWEEN start_date AND end_date
    AND experiment_id = experiment_name
    AND bucketing_id_type = bucketing_id_type
    AND event_id in ("purchase_NDCG", "rich_search_events_w_purchase")
)

select variant_id, _date, sum(event_value) as sum_pndcg
from `etsy-search-ml-dev.yzhang.online_pndcg`
where event_id = "purchase_NDCG"
group by variant_id, _date
order by variant_id, _date

select variant_id, _date, sum(event_value) as sum_purchase_events
from `etsy-search-ml-dev.yzhang.online_pndcg`
where event_id = "rich_search_events_w_purchase"
group by variant_id, _date
order by variant_id, _date


----  Offline
-- etsyweb nrv2_unified_ranking_try2
-- mmx nrv2_us_intl_v2_si
-- DAG: https://github.com/etsy/airflow/blob/d4b2cac9a8cf110856c1b9440d522a34b43b17bd/environments/prod/dags/search/ranking/nrv2_us_intl_v2_si_daily.py
select evalDate, count(*), avg(metrics.purchase.ndcg48)
from `etsy-search-ml-prod.search_ranking.second_pass_eval`
where evalDate between start_date and end_date
-- and modelName = "nrv2-us-intl-v2-si" -- on
and modelName = "nrv2-us-intl-si" -- off
and source in ("web_purchase", "boe_purchase")
group by evalDate
order by evalDate



----  Trying to see if it's possible to match up
DECLARE start_date DATE DEFAULT "2025-05-23";
DECLARE end_date DATE DEFAULT "2025-06-04";
DECLARE experiment_name STRING DEFAULT "ranking/search.mmx.2025_q2.nrv2_unified_ranking_try2";
-- DECLARE is_filtered BOOLEAN DEFAULT FALSE;
-- DECLARE bucketing_id_type INT64 DEFAULT 1;

-- get event mmx request ID from beacon
CREATE OR REPLACE TABLE `etsy-search-ml-dev.yzhang.vscm_with_mmx_id` AS (
    with browserCustomEvents as (
        select 
            visit_id, 
            SPLIT(visit_id, ".")[OFFSET(0)] as browser_id, 
            sequence_number, 
            event_name as event_id, 
            event_data, 
            event_timestamp, 
            _date
        from `etsy-data-warehouse-prod.catapult.visit_segment_custom_metrics`
        where event_timestamp IS NOT NULL 
        and _date = end_date
        and event_name = "purchase_NDCG"
    ),
    beacon as (
        select distinct
            visit_id,
            sequence_number,
            (select value from unnest(beacon.properties.key_value) where key = 'mmx_request_uuid') as mmx_request_uuid
        from `etsy-visit-pipe-prod.canonical.visit_id_beacons`
        where _PARTITIONTIME BETWEEN TIMESTAMP(start_date) AND TIMESTAMP(end_date)
        and beacon.event_name = "search"
    )
    select 
        bce.visit_id, 
        SPLIT(bce.visit_id, ".")[OFFSET(0)] as browser_id, 
        bce.sequence_number, 
        mmx_request_uuid,
        event_id, 
        event_data, 
        event_timestamp, 
        _date
    from browserCustomEvents bce
    left join beacon
    on bce.visit_id = beacon.visit_id
    and bce.sequence_number = beacon.sequence_number
)

select count(*)
from `etsy-search-ml-dev.yzhang.vscm_with_mmx_id`
where mmx_request_uuid is not null
-- can only match 4894 events, significantly lower than online numbers

