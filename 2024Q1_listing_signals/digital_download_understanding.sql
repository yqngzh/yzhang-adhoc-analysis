SELECT is_digital, count(*) as cnt
FROM `etsy-data-warehouse-prod.rollups.query_level_metrics_raw`
group by is_digital
-- 821,217,709 raw queries
-- 789322656 non digital
-- 31895053 (3.9%) digital

SELECT query_raw, query_normalized
FROM `etsy-data-warehouse-prod.rollups.query_level_metrics_raw`
where is_digital = 0
and lower(query_raw) like "%plan%"
-- 1320269 pdf
-- 7286987 plan
-- 2388671 stl
-- 497967 overlay
-- 9555877 pattern
-- 3690968 template
-- 8029912 design
-- 611236 emote
-- 5627255 poster
-- 889751 font
-- 16174782 print
-- 234194 svg
-- 132401 png
-- 744292 chart


create or replace table `etsy-sr-etl-prod.yzhang.dd_0301_0305` as (
    with beacon as (
        select
            (select value from unnest(beacon.properties.key_value) where key = 'query') as query,
            (select value from unnest(beacon.properties.key_value) where key = 'mmx_request_uuid') as mmx_request_uuid,
            listing_id.element as listing_id,
            pos,
        from `etsy-visit-pipe-prod.canonical.visit_id_beacons`,
            unnest(beacon.listing_ids.list) listing_id with offset as pos
        where (_PARTITIONTIME >= TIMESTAMP('2024-03-01') AND _PARTITIONTIME <= TIMESTAMP('2024-03-15'))
        and beacon.event_name = "search"
    ),
    attrs as (
        select
            requestUUID,
            candidateId,
            candidatePosition,
            attributions,
        from `etsy-sr-etl-prod.etl_data.search_attribution_v2`
        where timestamp_seconds(run_date) between TIMESTAMP('2024-03-01') and TIMESTAMP('2024-03-15')
    ),
    listing_data as (
        select key, listingWeb_isDigital
        from `etsy-ml-systems-prod.feature_bank_v2.listing_feature_bank_2024-03-15`
    ),
    full_data as (
        select 
            beacon.*, 
            listingWeb_isDigital,
            attrs.attributions
        from beacon
        join attrs 
        on (
            beacon.mmx_request_uuid = attrs.requestUUID
            and beacon.listing_id = attrs.candidateId
            and beacon.pos = attrs.candidatePosition
        )
        left join listing_data
        on beacon.listing_id = listing_data.key
    )
    select 
        mmx_request_uuid, query, listing_id, listingWeb_isDigital as listing_is_digital
    from full_data
    where "purchase" in unnest(attributions)
)
