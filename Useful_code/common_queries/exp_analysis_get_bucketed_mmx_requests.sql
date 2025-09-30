-- https://gist.etsycloud.com/mmatsui@etsy.com/ef124b19a1394b9d96eef3fb7d693bee

DECLARE start_date DATE DEFAULT '2025-09-28';
DECLARE end_date DATE DEFAULT '2025-09-29';
DECLARE config_flags ARRAY<STRING> DEFAULT ['ranking/search.mmx.2025_q3.sr_torch_ranker'];

with bucketing as (
 	select
    experiment_id AS config_flag, 
    bucketing_id,
    variant_id, 
    bucketing_ts
  from `etsy-data-warehouse-prod.catapult_unified.bucketing_period` 
  where _date = end_date 
  	and experiment_id in unnest(config_flags)
)
select
  date(timestamp_millis(beacon.timestamp)) as _date, 
  b.visit_id,
  bucketing_id,
  variant_id,
  (select value from unnest(beacon.properties.key_value) where key = 'query') as query,
  (select value from unnest(beacon.properties.key_value) where key = 'mmx_request_uuid') as mmx_request_uuid,
  (select safe_cast(value as int64) from unnest(beacon.properties.key_value) where key = 'page') as page_num,
  (select value from unnest(beacon.properties.key_value) where key = 'search_placement') as placement, 
from `etsy-visit-pipe-prod.canonical.visit_id_beacons` b
join bucketing 
  on b.beacon.browser_id = bucketing.bucketing_id 
  and timestamp_millis(b.beacon.timestamp) >= bucketing.bucketing_ts
where
  date(_partitiontime) between start_date and end_date
  and beacon.event_name = 'search'
  and (select value from unnest(beacon.properties.key_value) where key = 'search_placement') in ('wsg', 'allsr')

limit 1000