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
        from `etsy-search-ml-prod.search_ranking.second_pass_eval`
        where evalDate between date(start_date) and date(end_date)
        and source in ("web_purchase", "boe_purchase")
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


--- Get pNDCG anyways
select 
    modelName,
    avg(metrics.purchase.ndcg48) as avg_pndcg48, 
from `etsy-search-ml-prod.search_ranking.second_pass_eval`
where source in ("web_purchase", "boe_purchase")
and evalDate = date(study_date)
and modelName in (
    "nrv2-us-intl-v2-si",
    "nrv2-us-intl-si"
)
group by modelName

select 
    variant_id, modelName, 
    avg(avg_pndcg48) as avg_pndcg48
from `etsy-search-ml-dev.yzhang.unified_try2_offline`
group by variant_id, modelName
order by variant_id, modelName

with n_requests_table as (
    select variant_id, sum(event_value) as n_requests
    from `etsy-search-ml-dev.yzhang.unified_try2_online`
    where event_id = "rich_search_events_w_purchase"
    group by variant_id
),
pndcg_table as (
    select variant_id, sum(event_value) as pndcg
    from `etsy-search-ml-dev.yzhang.unified_try2_online`
    where event_id = "purchase_NDCG"
    group by variant_id
)
select pndcg_table.variant_id, pndcg / n_requests 
from n_requests_table
join pndcg_table using (variant_id)



---- Try only checking number of requests per day, not thinking about variants
-- offline
with requests_info as (
    select distinct 
        requestUUID,
        TIMESTAMP_MILLIS(timeSinceEpochMs) as event_timestamp,
        SPLIT(visitId, ".")[OFFSET(0)] as browserId,
    from `etsy-ml-systems-prod.attributed_instance.query_pipeline_web_organic_tight_2025_05_30`
    where "purchase" in unnest(attributions)
    union all
    select distinct 
        requestUUID,
        TIMESTAMP_MILLIS(timeSinceEpochMs) as event_timestamp,
        SPLIT(visitId, ".")[OFFSET(0)] as browserId
    from `etsy-ml-systems-prod.attributed_instance.query_pipeline_boe_organic_tight_2025_05_30`
    where "purchase" in unnest(attributions)
),
requests_info_clean as (
    select distinct requestUUID, event_timestamp, browserId
    from requests_info
    where event_timestamp is not null
    and browserId is not null
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
    where _date = "2025-05-30"
    and bucketing_id_type = 1
    and experiment_id = "ranking/search.mmx.2025_q2.nrv2_unified_ranking_try2"
)
select count(distinct requestUUID)
from requests_info_clean offres
join browserBucketingPeriod bbp
on (
    offres.browserId = bbp.bucketing_id
    and offres.event_timestamp >= bbp.bucketing_ts
)
-- offline no event time check 88461
-- offline with event time check 67157

-- offline try bucketing table instead of bucketingPeriod
with requests_info as (
    select distinct 
        requestUUID,
        TIMESTAMP_MILLIS(timeSinceEpochMs) as event_timestamp,
        SPLIT(visitId, ".")[OFFSET(0)] as browserId,
    from `etsy-ml-systems-prod.attributed_instance.query_pipeline_web_organic_tight_2025_05_30`
    where "purchase" in unnest(attributions)
    union all
    select distinct 
        requestUUID,
        TIMESTAMP_MILLIS(timeSinceEpochMs) as event_timestamp,
        SPLIT(visitId, ".")[OFFSET(0)] as browserId
    from `etsy-ml-systems-prod.attributed_instance.query_pipeline_boe_organic_tight_2025_05_30`
    where "purchase" in unnest(attributions)
),
requests_info_clean as (
    select distinct requestUUID, event_timestamp, browserId
    from requests_info
    where event_timestamp is not null
    and browserId is not null
),
firstBucketing as (
    SELECT
        bucketing_id,
        variant_id,
        MIN(bucketing_ts) AS bucketing_ts,
    FROM `etsy-data-warehouse-prod.catapult_unified.bucketing`
    -- where _date = "2025-05-30"
    WHERE _date between "2025-05-23" and "2025-05-30"
    and experiment_id = "ranking/search.mmx.2025_q2.nrv2_unified_ranking_try2"
    GROUP BY bucketing_id, variant_id
)
select count(distinct requestUUID)
from requests_info_clean offres
join firstBucketing bbp
on (
    offres.browserId = bbp.bucketing_id
    and offres.event_timestamp >= bbp.bucketing_ts
)
-- date = 0530 + time check: 53190
-- date = 0530: 88444
---- equivalent to using bucketing_period
-- date between start and 0530 + time check: 67157
-- date between start and 0530: 88461

-- online
with catapult_data as (
    SELECT
        _date,
        boundary_start_ts,
        variant_id,
        bucketing_id,
        bucketing_ts,
        event_id,
        event_value,
    FROM `etsy-data-warehouse-prod.catapult_unified.aggregated_event_daily`
    WHERE _date = "2025-05-30"
    and experiment_id = "ranking/search.mmx.2025_q2.nrv2_unified_ranking_try2"
    AND bucketing_id_type = 1
    AND event_id in ("purchase_NDCG", "rich_search_events_w_purchase")
)
select sum(event_value)
from catapult_data
where event_id = "rich_search_events_w_purchase"
-- online 82407
