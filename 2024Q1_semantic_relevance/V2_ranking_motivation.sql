------  All sampled search requests
------ 
with sampled_relevance_pred as (
    select 
        reqs.guid,
        reqs.query,
        reqs.listingId,
        queryBin,
        qisClass,
        platform,
        reqs.pageNum,
        reqs.rankingRank,
        metrics.classId
    from `etsy-data-warehouse-prod.search.sem_rel_query_listing_metrics` metrics
    join `etsy-data-warehouse-prod.search.sem_rel_hydrated_daily_requests` reqs
    using (tableUUID)
    where reqs.date = date('2024-07-08')
    and reqs.pageNum is not null
),
web_top3page_reqs as (
    select * from sampled_relevance_pred
    where pageNum is not null and platform = 'web'
),
web_top1page_reqs as (
    select * from web_top3page_reqs where pageNum = 1
),
web_top2row_reqs as (
    select * from web_top1page_reqs where rankingRank between 0 and 7
),
mweb_top3page_reqs as (
    select * from sampled_relevance_pred
    where pageNum is not null and platform = 'mweb'
),
mweb_top1page_reqs as (
    select * from mweb_top3page_reqs where pageNum = 1
),
mweb_top2row_reqs as (
    select * from mweb_top1page_reqs where rankingRank between 0 and 3
),
web_total_top3page_reqs as (
    select * from sampled_relevance_pred
    where pageNum is not null and platform in ('web', 'mweb')
),
web_total_top1page_reqs as (
    select * from web_total_top3page_reqs where pageNum = 1
),
boe_top3page_reqs as (
    select * from sampled_relevance_pred
    where pageNum is not null and platform = 'boe'
),
boe_top1page_reqs as (
    select * from boe_top3page_reqs where pageNum = 1
),
boe_top2row_reqs as (
    select * from boe_top1page_reqs where rankingRank between 0 and 3
)
select count(*) from web_top1page_reqs
where qisClass = 'broad'
and classId = 1

-- segment by intent specificity
select qisClass, classId, count(*) from web_total_top1page_reqs
group by qisClass, classId
order by qisClass, classId


-- QIS distribution in sampled traffic
with web_events_qis as (
    select distinct
        reqs.guid,
        reqs.query,
        qisClass,
    from `etsy-data-warehouse-prod.search.sem_rel_query_listing_metrics` metrics
    join `etsy-data-warehouse-prod.search.sem_rel_hydrated_daily_requests` reqs
    using (tableUUID)
    where reqs.date = date('2024-07-08')
    and reqs.pageNum is not null
    and platform in ('web', 'mweb')
)
select qisClass, count(*) as cnt
from web_events_qis
group by qisClass


--- QIS attributed GMS past 3 months
with qsn as (
    select query, sum(attributed_gms) as attributed_gms
    from `etsy-data-warehouse-prod.search.query_sessions_new`
    where _date between ('2024-04-08') and ('2024-07-08')
    group by query
),
qis as (
    select 
        query_raw query,
        CASE 
            WHEN class_id = 0 THEN 'broad' 
            WHEN class_id = 1 THEN 'direct_unspecified'
            WHEN class_id = 2 THEN 'direct_specified'
        END AS qisClass
    from `etsy-search-ml-prod.mission_understanding.qis_scores`
),
merged as (
    select qsn.query, attributed_gms, qisClass
    from qsn
    left join qis
    using (query)
)
select qisClass, sum(attributed_gms) as total_attributed_gms
from merged
group by qisClass


--- queries with historical purchase in past 100 days
with qsn as (
    select query, max(has_purchase) as has_purchase
    from `etsy-data-warehouse-prod.search.query_sessions_new`
    where _date between ('2024-04-01') and ('2024-07-08')
    group by query
),
sampled_relevance_pred as (
    select 
        reqs.guid,
        reqs.query,
        reqs.listingId,
        queryBin,
        qisClass,
        platform,
        reqs.pageNum,
        reqs.rankingRank,
        metrics.classId
    from `etsy-data-warehouse-prod.search.sem_rel_query_listing_metrics` metrics
    join `etsy-data-warehouse-prod.search.sem_rel_hydrated_daily_requests` reqs
    using (tableUUID)
    where reqs.date = date('2024-07-08')
    and reqs.pageNum is not null
),
sampled_relevance_pred_with_purchase as (
    select srp.*, has_purchase
    from sampled_relevance_pred srp
    left join qsn
    using (query)
),
web_total_top3page_reqs as (
    select * from sampled_relevance_pred_with_purchase
    where pageNum is not null and platform in ('web', 'mweb')
),
web_total_top1page_reqs as (
    select * from web_total_top3page_reqs where pageNum = 1
),
boe_top3page_reqs as (
    select * from sampled_relevance_pred_with_purchase
    where pageNum is not null and platform = 'boe'
),
boe_top1page_reqs as (
    select * from boe_top3page_reqs where pageNum = 1
)
select has_purchase, classId, count(*) from web_total_top1page_reqs
group by has_purchase, classId
order by has_purchase, classId


-- queries with and without purchase, percent in sampled traffic
with qsn as (
    select query, max(has_purchase) as has_purchase
    from `etsy-data-warehouse-prod.search.query_sessions_new`
    where _date between ('2024-04-01') and ('2024-07-08')
    group by query
),
web_reqs as (
    select distinct
        reqs.guid,
        reqs.query,
        -- qisClass
    from `etsy-data-warehouse-prod.search.sem_rel_query_listing_metrics` metrics
    join `etsy-data-warehouse-prod.search.sem_rel_hydrated_daily_requests` reqs
    using (tableUUID)
    where reqs.date = date('2024-07-08')
    and reqs.pageNum is not null
    and platform in ('web', 'mweb')
),
web_reqs_purchase as (
    select web_reqs.*, has_purchase
    from web_reqs
    left join qsn
    using (query)
)
select has_purchase, count(*) as cnt
from web_reqs_purchase
group by has_purchase

select qisClass, has_purchase, count(*) as cnt
from web_reqs_purchase
group by qisClass, has_purchase
order by qisClass, has_purchase




------  Purchase requests
------ 
-- Using jg-sem-rel-bq branch, write out 1 day of eval data (tight attribution)
-- Branch yzhang/relv2-data-analysis (modified output path from jg-sem-rel-bq)
-- purchase_reqs => sem_rel_purchase_reqs_2024-07-08
CREATE OR REPLACE EXTERNAL TABLE `etsy-sr-etl-prod.yzhang.sem_rel_purchase_reqs_with_uuids_2024-07-08`
OPTIONS (
    format = 'parquet',
    uris = ['gs://training-dev-search-data-jtzn/user/yzhang/semantic_relevance/purchase_reqs_with_uuid/2024_07_08*.parquet']
)
-- total 4134729 query listing pairs 


with purchased_pairs as (
    select 
        query, title, listing_id, attributions, sem_rel_softmax_0, sem_rel_softmax_3,
        CASE
            WHEN (sem_rel_softmax_3 > sem_rel_softmax_2 AND sem_rel_softmax_3 > sem_rel_softmax_1 AND sem_rel_softmax_3 > sem_rel_softmax_0) THEN 'Relevant'
            WHEN (sem_rel_softmax_2 > sem_rel_softmax_0 AND sem_rel_softmax_2 > sem_rel_softmax_3) THEN 'Partial'
            WHEN (sem_rel_softmax_1 > sem_rel_softmax_0 AND sem_rel_softmax_1 > sem_rel_softmax_3) THEN 'Partial'
            WHEN (sem_rel_softmax_0 > sem_rel_softmax_3 AND sem_rel_softmax_0 > sem_rel_softmax_2 AND sem_rel_softmax_0 > sem_rel_softmax_1) THEN 'Irrelevant'
            ELSE "Missing"
        END AS sem_rel_label
    from `etsy-sr-etl-prod.yzhang.sem_rel_purchase_reqs_2024-07-08`,
        unnest(attributions.list) as attr
    where attr.item = "purchase"
    and query != '' and query is not null
),
qis as (
    select 
        query_raw query,
        CASE 
            WHEN class_id = 0 THEN 'broad' 
            WHEN class_id = 1 THEN 'direct_unspecified'
            WHEN class_id = 2 THEN 'direct_specified'
        END AS qisClass
    from `etsy-search-ml-prod.mission_understanding.qis_scores`
),
purchased_pairs_qis as (
    select p.*, qisClass
    from purchased_pairs p
    left join qis
    using (query)
)
select sem_rel_label, count(*) 
from purchased_pairs_qis
where qisClass = 'broad' 
group by sem_rel_label
-- 120701 pairs purchased 

select qisClass, count(*) 
from purchased_pairs_qis
group by qisClass


select count(distinct request_uuid) 
from `etsy-sr-etl-prod.yzhang.sem_rel_purchase_reqs_with_uuids_2024-07-08`,
  unnest(attributions.list) as attr
where attr.item = "purchase"
and query != "" and query is not null

-- 117498 distinct request uuids, the same total 4134729 query listing pairs 
with purchased_pairs as (
    select 
        request_uuid,
        query, title, listing_id, attributions, sem_rel_softmax_0, sem_rel_softmax_3,
        CASE
            WHEN (sem_rel_softmax_3 > sem_rel_softmax_2 AND sem_rel_softmax_3 > sem_rel_softmax_1 AND sem_rel_softmax_3 > sem_rel_softmax_0) THEN 4
            WHEN (sem_rel_softmax_2 > sem_rel_softmax_0 AND sem_rel_softmax_2 > sem_rel_softmax_3) THEN 2
            WHEN (sem_rel_softmax_1 > sem_rel_softmax_0 AND sem_rel_softmax_1 > sem_rel_softmax_3) THEN 2
            WHEN (sem_rel_softmax_0 > sem_rel_softmax_3 AND sem_rel_softmax_0 > sem_rel_softmax_2 AND sem_rel_softmax_0 > sem_rel_softmax_1) THEN 1
            ELSE -1
        END AS sem_rel_label
    from `etsy-sr-etl-prod.yzhang.sem_rel_purchase_reqs_with_uuids_2024-07-08`,
        unnest(attributions.list) as attr
    where attr.item = "purchase"
    and query != "" and query is not null
),
qis as (
    select 
        query_raw query,
        CASE 
            WHEN class_id = 0 THEN 'broad' 
            WHEN class_id = 1 THEN 'direct_unspecified'
            WHEN class_id = 2 THEN 'direct_specified'
        END AS qisClass
    from `etsy-search-ml-prod.mission_understanding.qis_scores`
),
purchased_pairs_qis as (
    select p.*, qisClass
    from purchased_pairs p
    left join qis
    using (query)
),
request_level_rel as (
    select request_uuid, query, qisClass, min(sem_rel_label) as min_sem_rel_label
    from purchased_pairs_qis
    group by request_uuid, query, qisClass
    -- min(sem_rel_label): if there exists irrelevance => irrelevance; else if there exists partial => partial; else relevant
)
select min_sem_rel_label, count(*) 
from request_level_rel
where qisClass = 'broad' 
group by min_sem_rel_label
order by min_sem_rel_label desc

select qisClass, count(*) 
from request_level_rel
group by qisClass


-- sample random purchased query listing pairs from each relevance class
with purchased_pairs as (
    select 
        request_uuid,
        query, title, listing_id, attributions, sem_rel_softmax_0, sem_rel_softmax_3,
        CASE
            WHEN (sem_rel_softmax_3 > sem_rel_softmax_2 AND sem_rel_softmax_3 > sem_rel_softmax_1 AND sem_rel_softmax_3 > sem_rel_softmax_0) THEN 'Relevant'
            WHEN (sem_rel_softmax_2 > sem_rel_softmax_0 AND sem_rel_softmax_2 > sem_rel_softmax_3) THEN 'Partial'
            WHEN (sem_rel_softmax_1 > sem_rel_softmax_0 AND sem_rel_softmax_1 > sem_rel_softmax_3) THEN 'Partial'
            WHEN (sem_rel_softmax_0 > sem_rel_softmax_3 AND sem_rel_softmax_0 > sem_rel_softmax_2 AND sem_rel_softmax_0 > sem_rel_softmax_1) THEN 'Irrelevant'
            ELSE "Missing"
        END AS sem_rel_label
    from `etsy-sr-etl-prod.yzhang.sem_rel_purchase_reqs_with_uuids_2024-07-08`,
        unnest(attributions.list) as attr
    where attr.item = "purchase"
    and query != "" and query is not null
),
qis as (
    select 
        query_raw query,
        CASE 
            WHEN class_id = 0 THEN 'broad' 
            WHEN class_id = 1 THEN 'direct_unspecified'
            WHEN class_id = 2 THEN 'direct_specified'
        END AS qisClass
    from `etsy-search-ml-prod.mission_understanding.qis_scores`
),
purchased_pairs_qis as (
    select p.*, qisClass
    from purchased_pairs p
    left join qis
    using (query)
)
select query, qisClass, listing_id, title, sem_rel_label, sem_rel_softmax_0, sem_rel_softmax_3
from purchased_pairs_qis
where sem_rel_label = 'Relevant'
and rand() > 0.9
limit 50
