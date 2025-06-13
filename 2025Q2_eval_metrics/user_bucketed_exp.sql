---- User bucketed experiments: 
-- categorical embedding + buyer360: https://atlas.etsycorp.com/catapult/1364683737728
DECLARE start_date DATE DEFAULT "2025-04-08";
DECLARE end_date DATE DEFAULT "2025-04-22";
DECLARE experiment_name STRING DEFAULT "ranking/search.mmx.2025_q2.nrv2_unif_emb_b360_si";
DECLARE study_date DATE DEFAULT "2025-04-11";

-- offline, for study_date = 2025-04-11 only
create or replace table `etsy-search-ml-dev.yzhang.unif_emb_offline_daily` as (
    with offline_requests as (
        select 
            modelName, 
            tags.userId userId,
            requestUUID,
            avg(metrics.purchase.ndcg10) as avg_pndcg10, 
            avg(metrics.purchase.ndcg48) as avg_pndcg48, 
            -- avg(metrics.purchase.dcgAttributedPrice10) as avg_ppdcg10,
            -- avg(metrics.purchase.dcgAttributedPrice48) as avg_ppdcg48
        from `etsy-search-ml-prod.search_ranking.second_pass_eval`
        where evalDate between date(start_date) and date(end_date)
        and source in ("web_purchase", "boe_purchase")
        and tags.userId > 0
        and evalDate = date(study_date)
        and modelName in (
            "nrv2-unif-emb-b360-si",
            "nrv2-unif-emb-si",
            -- "nrv2_no_boarda_tm_si",
            "nrv2-us-intl-si"
        )
        group by modelName, tags.userId, requestUUID
        order by modelName, tags.userId, requestUUID
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
        and bucketing_id_type = 2  -- user bucketing
        and experiment_id = experiment_name
    )
    select *
    from offline_requests offres
    join browserBucketingPeriod bbp
    on (
        cast(offres.userId as string) = bbp.bucketing_id
        -- and TIMESTAMP(offres.evalDate) >= bbp.bucketing_ts -- no request timestamp stored in eval
        -- and TIMESTAMP(offres.evalDate) >= bbp.boundary_start_ts
    )
)

select variant_id, modelName, count(*)
from `etsy-search-ml-dev.yzhang.unif_emb_offline_daily`
group by variant_id, modelName
order by variant_id, modelName


-- online, for study_date = 2025-04-11 only
CREATE OR REPLACE TABLE `etsy-search-ml-dev.yzhang.unif_emb_online_daily` AS (
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
    AND bucketing_id_type = 2
    AND event_id in ("purchase_NDCG", "rich_search_events_w_purchase")
)

select variant_id, _date, sum(event_value)
from `etsy-search-ml-dev.yzhang.unif_emb_online_daily`
where event_id = "rich_search_events_w_purchase"
group by variant_id, _date
order by _date, variant_id

--- Trying to match number of events with a purchase. Getting very close.
-- on date 4/11/2025, 
-- variant_id         | offline_n_event_w_purchase | online_n_event_w_purchase
-- off                | 20224                      | 18314
-- unified_embedding  | 19692                      | 17856
-- buyer360           | 19790                      | 17881

-- likely due to event filtering from catapult (purchase must come after first bucketing ts), 
-- why not get the timestamp?
-- earliest stored attributed instance data (with request timestamp): 2025-05-13
-- latest user-bucketed experiment: Query Volume Weighting, ended on 2025-05-06
-- no longer have request timestamp in bq to query

select modelName, avg(avg_pndcg48)
from `etsy-search-ml-dev.yzhang.unif_emb_offline_daily`
group by modelName
order by modelName

select variant_id, modelName, avg(avg_pndcg48)
from `etsy-search-ml-dev.yzhang.unif_emb_offline_daily`
group by variant_id, modelName
order by variant_id, modelName

with n_requests_table as (
    select variant_id, sum(event_value) as n_requests
    from `etsy-search-ml-dev.yzhang.unif_emb_online_daily`
    where event_id = "rich_search_events_w_purchase"
    group by variant_id
),
pndcg_table as (
    select variant_id, sum(event_value) as pndcg
    from `etsy-search-ml-dev.yzhang.unif_emb_online_daily`
    where event_id = "purchase_NDCG"
    group by variant_id
)
select pndcg_table.variant_id, pndcg / n_requests 
from n_requests_table
join pndcg_table using (variant_id)

-- variant_id         offline_pNDCG@10 (SR) | offline_pNDCG@48 (SR)  | offline_pNDCG@48 (match) | online_pNDCG
-- off                0.4819                | 0.5441                 | 0.5469                   | 0.4659
-- unified_embedding  0.4847 (0.58%)        | 0.5458 (0.31%)         | 0.5464 (-0.09%)          | 0.4706 (1.0%)
-- buyer360           0.4872 (1.1%)         | 0.5478 (0.68%)         | 0.5465 (-0.07%)          | 0.4702 (0.92%)


