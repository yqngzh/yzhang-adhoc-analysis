-------- Query BERT
-- step 1. dataflow to generate query bert data 
-- read from GCS gs://etldata-prod-prolist-data-hkwv8r/data/outgoing/arizona/query_classifier_prolist_3mo/20231108
-- write to etsy-sr-etl-prod:yzhang.query_bert_taxo_2023_11_08
-- sanity check rows in data: 1,151,803, same across az and bq


-- step 2. query data
create or replace table `etsy-sr-etl-prod.yzhang.qtd_bert_distrib_match_query_data` as (
    with query_fb_data as (
        select `key` as query, queryLevelMetrics_bin as query_bin
        from `etsy-ml-systems-prod.feature_bank_v2.query_feature_bank_2023-11-09`
    ),
    query_intent_data as (
        select query_raw, inference.label as query_intent
        from `etsy-data-warehouse-prod.arizona.query_intent_labels`
        QUALIFY ROW_NUMBER() OVER(PARTITION BY query_raw ORDER BY inference.confidence DESC) = 1
    )
    select 
        qb.query, qb.taxonomy as paths, qb.predicted_probability as probs,
        fb.query_bin, qi.query_intent
    from `etsy-sr-etl-prod.yzhang.query_bert_taxo_2023_11_08` qb
    left join query_fb_data fb
    on qb.query = fb.query
    left join query_intent_data qi
    on qb.query = qi.query_raw
)


-- step 3. query bert raw data for computing distribution dist
create or replace table `etsy-sr-etl-prod.yzhang.qtd_distrib_match_bert_raw` as (
    with ldata as (
        select `key` as listing_id, verticaListings_taxonomyPath as listing_taxo
        from `etsy-ml-systems-prod.feature_bank_v2.listing_feature_bank_2023-11-09`
    )
    select 
        boost.*,
        qdata.query_bin, qdata.query_intent, qdata.paths, qdata.probs,
        ldata.listing_taxo
    from `etsy-sr-etl-prod.kbekal_insights.qtd_boosting_bert` boost
    left join `etsy-sr-etl-prod.yzhang.qtd_bert_distrib_match_query_data` qdata
    on boost.query = qdata.query
    left join ldata
    on boost.listing_id = ldata.listing_id
)

-- sanity check: each left join maintains the number of rows
-- how many distinct request are in dataset (uuid, behavior)
-- -- 20352, half = 10176 control
-- how many distinct request have query taxonomy
SELECT count(distinct uuid, behavior) 
FROM `etsy-sr-etl-prod.yzhang.qtd_distrib_match_bert_raw` 
where array_length(paths) > 0
-- 7616, half = 3808


-- step 4. dataflow => qtd_distrib_match_bert_processed
-- sanity check 
-- 20352 requests in table
-- 7568 requests with distribution distance, control & variant half and half


-- step 4. analyze processed data
create or replace table `etsy-sr-etl-prod.yzhang.qtd_distrib_match_bert_full` as (
    select r.*, p.qtd_distrib, p.listing_taxo_distrib, p.distrib_distance
    from `etsy-sr-etl-prod.yzhang.qtd_distrib_match_bert_raw` r
    join `etsy-sr-etl-prod.yzhang.qtd_distrib_match_bert_processed` p
    on r.uuid = p.uuid
    and r.behavior = p.behavior
)

-- sanity check: same total number of rows, number of distinct requests as raw
-- spot check if distribution makes sense
SELECT paths, probs, qtd_distrib
FROM `etsy-sr-etl-prod.yzhang.qtd_distrib_match_bert_full` 
where distrib_distance is not null
and array_length(paths) between 3 and 10
-- spot check if distance calculation is as expected
SELECT qtd_distrib, listing_taxo_distrib, distrib_distance
FROM `etsy-sr-etl-prod.yzhang.qtd_distrib_match_bert_full` 
where distrib_distance is not null
and array_length(paths) between 3 and 10
-- check dist range
SELECT min(distrib_distance), max(distrib_distance)
FROM `etsy-sr-etl-prod.yzhang.qtd_distrib_match_bert_full` 
where distrib_distance is not null
-- 0, 1.999, as expected

-- average distribution distance across requests by variant
with tmp as (
    SELECT *
    FROM `etsy-sr-etl-prod.yzhang.qtd_distrib_match_bert_full` 
    where distrib_distance is not null
    and query_bin = 'top.01'
)
select behavior, 2.0 - avg(distrib_distance) as avg_distrib_closeness
from tmp
group by behavior
