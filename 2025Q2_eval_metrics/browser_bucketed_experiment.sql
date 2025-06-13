---- Recent browser bucketed experiments: 
-- unified try 2: https://atlas.etsycorp.com/catapult/1374147072828
DECLARE start_date DATE DEFAULT "2025-05-23";
DECLARE end_date DATE DEFAULT "2025-06-04";
DECLARE experiment_name STRING DEFAULT "ranking/search.mmx.2025_q2.nrv2_unified_ranking_try2";
DECLARE study_date DATE DEFAULT "2025-05-30";

create or replace table `etsy-search-ml-dev.yzhang.unified_try2_offline` as (
    with requests_info as (
        select distinct 
            requestUUID,
            TIMESTAMP_MILLIS(timeSinceEpochMs) as event_timestamp,
            SPLIT(visitId, ".")[OFFSET(0)] as browserId,
        from `etsy-ml-systems-prod.attributed_instance.query_pipeline_web_organic_tight_2025_05_30`
        union all
        select distinct 
            requestUUID,
            TIMESTAMP_MILLIS(timeSinceEpochMs) as event_timestamp,
            SPLIT(visitId, ".")[OFFSET(0)] as browserId
        from `etsy-ml-systems-prod.attributed_instance.query_pipeline_boe_organic_tight_2025_05_30`
    ),
    requests_info_clean as (
        select distinct requestUUID, event_timestamp, browserId
        from requests_info
        where event_timestamp is not null
        and browserId is not null
    ),
    offline_requests as (
        select 
            modelName,
            requestUUID,
            avg(metrics.purchase.ndcg10) as avg_pndcg10, 
            avg(metrics.purchase.ndcg48) as avg_pndcg48, 
            -- avg(metrics.purchase.dcgAttributedPrice10) as avg_ppdcg10,
            -- avg(metrics.purchase.dcgAttributedPrice48) as avg_ppdcg48
        from `etsy-search-ml-prod.search_ranking.second_pass_eval`
        where evalDate between date(start_date) and date(end_date)
        and source in ("web_purchase", "boe_purchase")
        -- and tags.userId > 0
        and evalDate = date(study_date)
        and modelName in (
            "nrv2-us-intl-v2-si",
            "nrv2-us-intl-si"
        )
        group by modelName, requestUUID
    ),
    offline_requests_full as (
        select 
            offline_requests.*,
            event_timestamp,
            browserId
        from offline_requests
        left join requests_info_clean using (requestUUID)
    ),
    browserBucketingPeriod as (
        select 
            experiment_id, 
            boundary_start_ts, 
            variant_id, 
            bucketing_id, 
            bucketing_id_type, 
            bucketing_ts, 
            filtered_bucketing_ts
        from `etsy-data-warehouse-prod.catapult_unified.bucketing_period`
        where _date = study_date
        and bucketing_id_type = 1
        and experiment_id = experiment_name
    )
    select *
    from offline_requests_full offres
    join browserBucketingPeriod bbp
    on (
        offres.browserId = bbp.bucketing_id
        and offres.event_timestamp >= bbp.bucketing_ts
        -- and TIMESTAMP(offres.evalDate) >= bbp.boundary_start_ts
    )
)

select variant_id, modelName, count(*)
from `etsy-search-ml-dev.yzhang.unified_try2_offline`
group by variant_id, modelName
order by variant_id, modelName


CREATE OR REPLACE TABLE `etsy-search-ml-dev.yzhang.unified_try2_online` AS (
    SELECT
        _date,
        boundary_start_ts,
        variant_id,
        bucketing_id,
        bucketing_ts,
        event_id,
        event_value,
    FROM `etsy-data-warehouse-prod.catapult_unified.aggregated_event_daily`
    WHERE _date = study_date
    AND experiment_id = experiment_name
    AND bucketing_id_type = 1
    AND event_id in ("purchase_NDCG", "rich_search_events_w_purchase")
)

select variant_id, sum(event_value)
from `etsy-search-ml-dev.yzhang.unified_try2_online`
where event_id = "rich_search_events_w_purchase"
group by variant_id
order by variant_id


--- Trying to match number of events with a purchase
-- variant_id         | offline_n_event_w_purchase | online_n_event_w_purchase
-- off                | 33878                      | 41317
-- on                 | 33279                      | 41090

-- offline does some additional filtering like missing mmx_request_uuid??
