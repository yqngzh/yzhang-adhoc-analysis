select 
  (select value from unnest(beacon.properties.key_value) where key = 'query') as query,
  (select value from unnest(beacon.properties.key_value) where key = 'translated_query') as translated_query,
  (select value from unnest(beacon.properties.key_value) where key = 'mmx_request_uuid') as mmx_request_uuid,
from `etsy-visit-pipe-prod.canonical.visit_id_beacons`
where _PARTITIONTIME BETWEEN TIMESTAMP('2024-02-08') AND TIMESTAMP('2024-02-09')
and beacon.event_name = "search"
and (select value from unnest(beacon.properties.key_value) where key = 'translated_query') is null
