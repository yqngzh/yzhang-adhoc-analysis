----  Precision, Recall
create or replace table `etsy-sr-etl-prod.yzhang.sem_rel_segment_analysis` as (
    with sampled_request as (
        select tableUUID, mmxRequestUUID, query, queryBin, qisClass, listingId, pageNum, platform
        from `etsy-data-warehouse-prod.search.sem_rel_hydrated_daily_requests`
        where date = date("2024-04-21")
    ),
    relevance_pred as (
        select tableUUID, classId
        from `etsy-data-warehouse-prod.search.sem_rel_query_listing_metrics`
        where date = date("2024-04-21")
    ),
    user_fb as (
        select `key` as user_id, 
            userSegmentFeatures_buyerSegment as buyer_segment
        from `etsy-ml-systems-prod.feature_bank_v2.user_feature_bank_2024-04-21`
    ),
    raw_rpc_log as (
        select
            request.query as query,
            date(queryTime) date,
            response.mmxRequestUUID as mmxRequestUUID,
            request.options.personalizationOptions.userId,
        from `etsy-searchinfra-gke-prod-2.thrift_mmx_listingsv2search_search.rpc_logs_*`
        where date(queryTime) = date("2024-04-21")
    ),
    rpc_with_user_segment as (
        select 
            rpc.*,
            if (rpc.userId > 0, u.buyer_segment, "Signed Out") as buyer_segment,
        from raw_rpc_log rpc
        left join user_fb u
        on rpc.userId = u.user_id
    )
    select
        sr.*,
        pred.classId,
        rpc_u.userId,
        rpc_u.buyer_segment
    from sampled_request sr
    join relevance_pred pred
    on sr.tableUUID = pred.tableUUID
    join rpc_with_user_segment rpc_u
    on (
        sr.mmxRequestUUID = rpc_u.mmxRequestUUID
        and sr.query = rpc_u.query
    )
)


--- sanity check: n requests by platform
with tmp as (
    select distinct mmxRequestUUID, platform
    from `etsy-sr-etl-prod.yzhang.sem_rel_segment_analysis`
    -- from `etsy-data-warehouse-prod.search.sem_rel_hydrated_daily_requests`
)
select platform, count(*) as n_requests
from tmp
group by platform
order by platform

with tmp as (
    select distinct mmxRequestUUID, queryBin, qisClass, buyer_segment, platform
    from `etsy-sr-etl-prod.yzhang.sem_rel_segment_analysis`
)
select platform, count(*) as n_requests
from tmp
group by platform
order by platform


with tmp as (
    select 
        mmxRequestUUID, queryBin, qisClass, buyer_segment, platform, classId,
        if(classId = 4, 1, 0) as exact_flag,
        if(classId = 1, 1, 0) as irr_flag,
    from `etsy-sr-etl-prod.yzhang.sem_rel_segment_analysis`
    where pageNum = 1
),
metrics as (
    select 
        mmxRequestUUID, queryBin, qisClass, buyer_segment, platform,
        sum(exact_flag) / count(*) as frac_exact,
        sum(irr_flag) / count(*) as frac_irr,
    from tmp
    group by mmxRequestUUID, queryBin, qisClass, buyer_segment, platform
)
-- select
--     platform,
--     avg(frac_exact) as avg_frac_exact,
--     avg(frac_irr) as avg_frac_irr
-- from metrics
-- group by platform
select
    queryBin,
    avg(frac_exact) as avg_frac_exact,
    avg(frac_irr) as avg_frac_irr
from metrics
where platform in ('boe')
group by queryBin
--- similar observation as offline precisions, 
--- on BOE, sign-in sign-out difference is less obvious

--- highest fraction of irrelevance: broad, missing
--- lowest fraction of irrelevance: direct_specified
--- highest fraction of relevance: direct_unspecified, broad
--- lowest fraction of relevance: direct_specified


----  NDCG
with sampled_request as (
    select guid, query, queryBin, qisClass, pageNum, platform
    from `etsy-data-warehouse-prod.search.sem_rel_hydrated_daily_requests`
    where date = date("2024-04-21")
),
ndcg_result as (
    select guid, relevanceNDCG10
    from `etsy-data-warehouse-prod.search.sem_rel_requests_metrics`
    where date = date("2024-04-21")
),
request_with_ndcg as (
    select sr.*, relevanceNDCG10 as ndcg10
    from sampled_request sr
    join ndcg_result nr
    on sr.guid = nr.guid
)
select queryBin, avg(ndcg10) as avg_ndcg10
from request_with_ndcg
where platform in ('boe')
-- and pageNum = 1
group by queryBin
order by queryBin

