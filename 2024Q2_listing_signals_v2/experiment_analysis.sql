DECLARE my_experiment STRING default "ranking/search.mmx.2024_q2.nrv2_listing_signal_web";
DECLARE start_date DATE default "2024-04-08";
DECLARE end_date DATE default "2024-04-17";

-- SELECT count(*) as num_bucketed_units
-- FROM `etsy-data-warehouse-prod.catapult_unified.bucketing_period`
-- WHERE _date = end_date
-- AND experiment_id = my_experiment
-- AND variant_id = "nrv2_listing_signal_web"


SELECT bucketing_id as browser_id
  FROM `etsy-data-warehouse-prod.catapult_unified.aggregated_segment_event`
  WHERE _date = end_date
  AND experiment_id = my_experiment
  AND event_id = "search"
  AND event_value = "New"
  AND variant_id = "on"

create or replace table `etsy-sr-etl-prod.yzhang.lsig_abtest` as (
  with browsers as (
    select
        bucketing_id as browser_id,
        variant_id,
    from `etsy-data-warehouse-prod.catapult_unified.bucketing_period`
    where _date = end_date
    and experiment_id = my_experiment
  )
  select
    b.*,
    (select value from unnest(beacon.properties.key_value) where key = 'query') as query,
    (select value from unnest(beacon.properties.key_value) where key = 'mmx_request_uuid') as mmx_request_uuid,
    listing_id.element as listing_id,
    pos,
  from browsers b
  join `etsy-visit-pipe-prod.canonical.visit_id_beacons` v using (visit_id)
  cross join unnest(beacon.listing_ids.list) listing_id with offset as pos
  where (v._PARTITIONTIME >= TIMESTAMP(start_date) AND v._PARTITIONTIME <= TIMESTAMP(end_date))
  and beacon.event_name = "search"
)