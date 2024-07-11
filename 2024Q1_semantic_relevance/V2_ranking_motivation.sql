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

