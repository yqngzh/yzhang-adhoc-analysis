-- coverage
select sum(queryLevelMetrics_gms)
from `etsy-ml-systems-prod.feature_bank_v2.query_feature_bank_2023-10-16` fb
join (
    select distinct query
    from `etsy-sr-etl-prod.yzhang.query_bert_taxo_2023_10_16`
) bert_taxo
on fb.key = bert_taxo.query


-- 2 day of rpc data
CREATE OR REPLACE TABLE `etsy-sr-etl-prod.yzhang.query_taxo_bert_temp_qdata` as (
    select `key` as query_str, 
        queryLevelMetrics_bin as query_bin,
        taxonomy as paths,
        predicted_probability as predicted_prob
    from `etsy-ml-systems-prod.feature_bank_v2.query_feature_bank_2023-10-16` qfb
    join `etsy-sr-etl-prod.yzhang.query_bert_taxo_2023_10_16` bert_taxo
    on qfb.key = bert_taxo.query
)

CREATE OR REPLACE TABLE `etsy-sr-etl-prod.yzhang.query_taxo_bert_temp_udata` as (
    select `key` as user_id, 
        userSegmentFeatures_buyerSegment as buyer_segment
    from `etsy-ml-systems-prod.feature_bank_v2.user_feature_bank_2023-10-16`
)

CREATE OR REPLACE TABLE `etsy-sr-etl-prod.yzhang.query_taxo_bert_temp_ldata` as (
    select 
        alb.listing_id, 
        lt.full_path
    from `etsy-data-warehouse-prod.rollups.active_listing_basics` alb
    join `etsy-data-warehouse-prod.materialized.listing_taxonomy` lt
    on alb.listing_id = lt.listing_id
    and alb.taxonomy_id = lt.taxonomy_id
)

CREATE OR REPLACE TABLE `etsy-sr-etl-prod.yzhang.query_taxo_bert_temp_qlgms` as (
    select *
    from `etsy-data-warehouse-prod.propensity.adjusted_query_listing_pairs`
    where platform = 'web' and region = 'US' and language = 'en-US'
    and _date >= DATE('2023-10-16')
    and _date <= DATE('2023-10-17')
)

CREATE OR REPLACE TABLE `etsy-sr-etl-prod.yzhang.query_taxo_bert_temp_rpc` AS (
    SELECT
        response.mmxRequestUUID,
        request.query,
        request.options.personalizationOptions.userId,
        CAST(request.offset / request.limit + 1 AS INTEGER) page_no,
        listingId,
        position,
        DATE(queryTime) as query_date
    FROM `etsy-searchinfra-gke-prod-2.thrift_mmx_listingsv2search_search.rpc_logs_*`,
        UNNEST(response.listingIds) AS listingId  WITH OFFSET position
    WHERE request.options.searchPlacement = "wsg"
    AND DATE(queryTime) >= DATE('2023-10-16')
    AND DATE(queryTime) <= DATE('2023-10-17')
    AND request.options.csrOrganic = TRUE
    AND (request.offset + request.limit) < 144
    AND request.options.mmxBehavior.matching IS NOT NULL
    AND request.options.mmxBehavior.ranking IS NOT NULL
    AND request.options.mmxBehavior.marketOptimization IS NOT NULL
)

CREATE OR REPLACE TABLE `etsy-sr-etl-prod.yzhang.query_taxo_bert_temp_step1` AS (
    select
        rpc_data.*,
        q.query_bin,
        q.paths,
        q.predicted_prob,
    from `etsy-sr-etl-prod.yzhang.query_taxo_bert_temp_rpc` rpc_data
    left join `etsy-sr-etl-prod.yzhang.query_taxo_bert_temp_qdata` q
    on rpc_data.query = q.query_str
)

CREATE OR REPLACE TABLE `etsy-sr-etl-prod.yzhang.query_taxo_bert_temp_step2` AS (
    select
        step1.*,
        if (step1.userId > 0, u.buyer_segment, "Signed Out") as buyer_segment,
    from `etsy-sr-etl-prod.yzhang.query_taxo_bert_temp_step1` step1
    left join `etsy-sr-etl-prod.yzhang.query_taxo_bert_temp_udata` u
    on step1.userId = u.user_id
)

CREATE OR REPLACE TABLE `etsy-sr-etl-prod.yzhang.query_taxo_bert_temp_step3` AS (
    select
        step2.*,
        l.full_path,
    from `etsy-sr-etl-prod.yzhang.query_taxo_bert_temp_step2` step2
    left join `etsy-sr-etl-prod.yzhang.query_taxo_bert_temp_ldata` l
    on step2.listingId = l.listing_id
)

CREATE OR REPLACE TABLE `etsy-sr-etl-prod.yzhang.query_taxo_bert_lastpass_rpc` AS (
    with qlg as (
        select _date, query, listingId, total_winsorized_gms as winsorized_gms
        from `etsy-sr-etl-prod.yzhang.query_taxo_bert_temp_qlgms`
    )
    select
        step3.*,
        qlg.winsorized_gms,
    from `etsy-sr-etl-prod.yzhang.query_taxo_bert_temp_step3` step3
    left join qlg
    on (
        qlg._date = step3.query_date and 
        qlg.query = step3.query and
        qlg.listingId = step3.listingId
    )
)


-- coverage
select count(distinct query)
from `etsy-sr-etl-prod.yzhang.query_taxo_bert_lastpass_rpc`
where array_length(paths) > 0


with rpc_query as (
  select distinct query
  from `etsy-sr-etl-prod.yzhang.query_taxo_bert_lastpass_rpc` rpc
  where array_length(paths) > 0
),
qgms as (
  select query, q.gms
  from rpc_query
  left join `etsy-data-warehouse-prod.rollups.query_level_metrics_raw` q
  on rpc_query.query = q.query_raw
)
select sum(gms) from qgms


-- sum of predicted probability
with sum_prob as (
  select sum(predicted_prob) as spp
  from `etsy-sr-etl-prod.yzhang.query_taxo_bert_lastpass_rpc` rpc,
    unnest(predicted_prob) as predicted_prob
  group by mmxRequestUUID, query, userId, listingId, query_date
)
select min(spp) as min_spp, max(spp) as max_spp
from sum_prob
-- min 0.795, max 1.98

-- sanity check dataflow
select 
    query, paths, predicted_prob, full_path,
    th0, th10, th25, th50, th75, th90, th100,
from `etsy-sr-etl-prod.yzhang.query_taxo_bert_lastpass_rpc_analysis`
where array_length(paths) between 1 and 5
limit 100

-- calculate gms at risk
SELECT sum(winsorized_gms) 
FROM `etsy-sr-etl-prod.yzhang.query_taxo_bert_lastpass_rpc_analysis_normalized`
where th0 = 'remove'
