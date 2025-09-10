---------------
-- semrel_boosting_global_threshold_search_events contains all visits & search events from the experiment
-- it's then joined with rpc logs 
-- https://console.cloud.google.com/bigquery?ws=!1m7!1m6!12m5!1m3!1setsy-bigquery-adhoc-prod!2sus-central1!3sf269db7d-1361-4608-b8f3-9e74e7e024d5!2e1
---------------

create or replace table `etsy-search-ml-dev.search.yzhang_semrel_boosting_global_threshold_engagement` as (
    with engage_data as (
        select 
            _date,
            visit_id,
            mmx_request_uuid,
            query,
            query_en,
            listing_id,
            display_price_usd,
            clicked,
            added_to_cart,
            purchase_info_tight.purchased as tight_purchased,
            purchase_info_tight.gms as tight_gms,
        from `etsy-data-warehouse-prod.rollups.search_impressions`
        where _date between date('2025-08-29') and date('2025-09-01')
        and query is not null and query != ""
    ),
    agg_engage_data_by_request as (
        select
            _date,
            visit_id,
            mmx_request_uuid,
            query,
            query_en,
            avg(display_price_usd) as avg_impression_price,
            sum(clicked) as clicks,
            sum(added_to_cart) as atcs,
            sum(tight_purchased) as tight_purchases,
            sum(tight_gms) as tight_gms,
        from engage_data
        group by _date, visit_id, mmx_request_uuid, query, query_en
    )
    select 
        ex.requestUUID,
        ex.query,
        eng.query_en as queryEn,
        ex.visit_id,
        ex.date_of_search,
        ex.page,
        ex.page_guid,
        ex.event_source,
        ex.mmx_behavior,
        ex.variant_id,
        ex.experiment_id,
        ex.ranking_ids,
        ex.lastpass_ids,
        ifnull(eng.avg_impression_price, 0) as avg_impression_price,
        ifnull(eng.clicks, 0) as clicks,
        ifnull(eng.atcs, 0) as atcs,
        ifnull(eng.tight_purchases, 0) as tight_purchases,
        ifnull(eng.tight_gms, 0) as tight_gms,
    from `etsy-sr-etl-prod.kbekal_insights.semrel_boosting_global_threshold_stage_listings` ex
    left join agg_engage_data_by_request eng
    on (
        eng.mmx_request_uuid = ex.requestUUID
        and eng.visit_id = ex.visit_id
        and eng._date = ex.date_of_search
        and eng.query = ex.query
    )
)


