-- example experiment: https://atlas.etsycorp.com/catapult/1374147072828
-- go/catapult-unified-enums


----  Get experiment boundary dates, event filtering, bucketing type
SELECT
    MAX(DATE(boundary_start_ts)) AS start_date,
    MAX(_date) AS end_date,
FROM `etsy-data-warehouse-prod.catapult_unified.experiment`
WHERE experiment_id = "ranking/search.mmx.2025_q2.nrv2_unified_ranking_try2"
-- start_date "2025-05-23"
-- end_date "2025-06-04"

SELECT
    is_filtered,
    bucketing_id_type,
FROM `etsy-data-warehouse-prod.catapult_unified.experiment`
WHERE _date = "2025-06-04"
AND experiment_id = "ranking/search.mmx.2025_q2.nrv2_unified_ranking_try2"
-- is_filtered = False
-- bucketing_id_type = 1 (browser)




----  Try to recreate topline metrics
CREATE OR REPLACE TABLE `etsy-search-ml-dev.yzhang.ab_first_bucket` AS (
    SELECT
        bucketing_id,
        bucketing_id_type,
        variant_id,
        bucketing_ts,
    FROM `etsy-data-warehouse-prod.catapult_unified.bucketing_period`
    WHERE _date = "2025-06-04"
    AND experiment_id = "ranking/search.mmx.2025_q2.nrv2_unified_ranking_try2"
);

CREATE OR REPLACE TABLE `etsy-search-ml-dev.yzhang.events` AS (
    SELECT * FROM UNNEST([
        "backend_cart_payment", -- conversion rate
        "total_winsorized_gms", -- winsorized acbv
        "prolist_total_spend",  -- prolist revenue
        "gms",                   -- note: gms data is in cents
        -- "purchase_NDCG"
    ]) AS event_id
);

CREATE OR REPLACE TABLE `etsy-search-ml-dev.yzhang.events_per_unit` AS (
    SELECT
        bucketing_id,
        variant_id,
        event_id,
        event_value
    FROM `etsy-data-warehouse-prod.catapult_unified.aggregated_event_func`("2025-05-23", "2025-06-04")
    JOIN `etsy-search-ml-dev.yzhang.events`
    USING (event_id)
    WHERE experiment_id = "ranking/search.mmx.2025_q2.nrv2_unified_ranking_try2"
);

CREATE OR REPLACE TABLE `etsy-search-ml-dev.yzhang.all_units_events` AS (
    SELECT
        bucketing_id,
        variant_id,
        event_id,
        COALESCE(event_value, 0) AS event_count,
    FROM `etsy-search-ml-dev.yzhang.ab_first_bucket`
    CROSS JOIN `etsy-search-ml-dev.yzhang.events`
    LEFT JOIN `etsy-search-ml-dev.yzhang.events_per_unit`
    USING(bucketing_id, variant_id, event_id)
);

SELECT
    event_id,
    variant_id,
    COUNT(*) AS total_units_in_variant,
    AVG(IF(event_count = 0, 0, 1)) AS percent_units_with_event,
    AVG(event_count) AS avg_events_per_unit,
    AVG(IF(event_count = 0, NULL, event_count)) AS avg_events_per_unit_with_event
FROM `etsy-search-ml-dev.yzhang.all_units_events`
GROUP BY event_id, variant_id
ORDER BY event_id, variant_id;




----  Try to recreate pNDCG
-- aggregated_event_level
CREATE OR REPLACE TABLE `etsy-search-ml-dev.yzhang.cumu_pndcg_over_time` AS (
    SELECT
        variant_id,
        event_id,
        event_value,
        segmentation,
        segment,
        bucketing_ids_with_event,
        bucketing_ids_in_variant
    FROM `etsy-data-warehouse-prod.catapult_unified.aggregated_event_level`
    WHERE _date = "2025-06-04"
    AND experiment_id = "ranking/search.mmx.2025_q2.nrv2_unified_ranking_try2"
    AND event_id in ("purchase_NDCG", "rich_search_events_w_purchase")
    ORDER BY event_id, variant_id
)

SELECT variant_id, event_id, event_value, bucketing_ids_with_event, bucketing_ids_in_variant
FROM `etsy-search-ml-dev.yzhang.cumu_pndcg_over_time`
WHERE segmentation = "any"
AND segment = "all"
order by variant_id, event_id
-- off: 2599591531.306 / 543639.0 = 4781.83
-- on:  2611979293.580 / 538041.0 = 4854.61
-- less people make purchase, but purchased item rank increased


-- aggregated_event_daily
CREATE OR REPLACE TABLE `etsy-search-ml-dev.yzhang.pndcg_agg_daily` AS (
    SELECT
        _date,
        boundary_start_ts,
        variant_id,
        bucketing_id,
        bucketing_ts,
        event_id,
        event_value,
    FROM `etsy-data-warehouse-prod.catapult_unified.aggregated_event_daily`
    WHERE _date BETWEEN "2025-05-23" AND "2025-06-04"
    AND experiment_id = "ranking/search.mmx.2025_q2.nrv2_unified_ranking_try2"
    AND bucketing_id_type = 1
    AND event_id in ("purchase_NDCG", "rich_search_events_w_purchase")
)

SELECT count(*) 
FROM `etsy-search-ml-dev.yzhang.pndcg_agg_daily`
where bucketing_ts < boundary_start_ts
-- 0

select variant_id, sum(event_value)
from `etsy-search-ml-dev.yzhang.pndcg_agg_daily`
where event_id = "purchase_NDCG"
group by variant_id
-- off: 2599591531.306 / 543639.0
-- on:  2611979293.580 / 538041.0


-- visit_segment_custom_metrics
-- https://github.com/etsy/sparkly/blob/7e69be1182b2971d0268a322be861dbdbdd2958c/spark-jobs/experimentation/src/main/scala/com/etsy/spark/jobs/catapult/unified/aggregators/BrowserCustomEventAggregator.scala
CREATE OR REPLACE TABLE `etsy-search-ml-dev.yzhang.vscm_to_agg_daily` AS (
    with browserBucketingPeriod as (
        select 
            experiment_id, 
            boundary_start_ts, 
            variant_id, 
            bucketing_id, 
            bucketing_id_type, 
            bucketing_ts, 
            filtered_bucketing_ts
        from `etsy-data-warehouse-prod.catapult_unified.bucketing_period`
        where _date = "2025-06-04"
        and bucketing_id_type = 1
        and experiment_id = "ranking/search.mmx.2025_q2.nrv2_unified_ranking_try2"
    ),
    browserCustomEvents as (
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
        and _date = "2025-06-04"
        and event_name = "purchase_NDCG"
    ),
    postBucketingBrowserCustomEvents as (
        select *
        from browserCustomEvents bce
        join browserBucketingPeriod bbp
        on (
            bce.browser_id = bbp.bucketing_id
            and bce.event_timestamp >= bbp.bucketing_ts 
            and bce._date >= date(bbp.boundary_start_ts)
        )
    ),
    tmp as (
        select 
            experiment_id, boundary_start_ts, variant_id, bucketing_id, 
            visit_id, sequence_number,
            bucketing_ts, event_id, event_data,
            ROW_NUMBER() OVER (
                PARTITION BY boundary_start_ts, experiment_id, variant_id, bucketing_id, event_id
                ORDER BY event_timestamp, visit_id, sequence_number
            ) AS row_number
        from postBucketingBrowserCustomEvents
    ),
    cleanedPostBucketingBrowserEvents as (
        select 
            experiment_id, boundary_start_ts, variant_id, bucketing_id, 
            bucketing_ts, event_id, event_data
        from tmp
        where row_number = 1
        or (row_number > 1 and sequence_number = 0)
    )
    select 
        experiment_id, boundary_start_ts, variant_id, 
        bucketing_id, bucketing_ts, event_id, 
        sum(event_data) as event_value
    from cleanedPostBucketingBrowserEvents
    group by experiment_id, boundary_start_ts, variant_id, bucketing_id, bucketing_ts, event_id
)

select variant_id, sum(event_value)
from `etsy-search-ml-dev.yzhang.pndcg_agg_daily`
where _date = "2025-06-04"
and event_id = "purchase_NDCG"
group by variant_id
-- off	216686228.3980912
-- on	223590694.61373359

select variant_id, sum(event_value)
from `etsy-search-ml-dev.yzhang.vscm_to_agg_daily`
group by variant_id
-- off	216686228.39810649
-- on	223590694.61375052
