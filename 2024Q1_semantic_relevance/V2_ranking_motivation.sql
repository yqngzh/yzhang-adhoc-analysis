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
    where reqs.date = date('2024-06-20')
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
select count(*) from web_top3page_reqs
where classId = 1


------ irrelevance percentage (where classId = 1)
-- web 
---- top 3 page: 12601 / 96602 = 13.04%
---- top 1 page: 8112 / 68115 = 11.91%
---- top 2 rows: 966 / 11329 = 8.53%

-- mweb
---- top 3 page: 13040 / 91688 = 14.22%
---- top 1 page: 7353 / 57713 = 12.74%
---- top 2 rows: 614 / 6836 = 8.98%

-- boe
---- top 3 page: 20368 / 115798 = 17.59%
---- top 1 page: 10734 / 67524 = 15.90%
---- top 2 rows: 1197 / 9649 = 12.41%



------ not relevant percentage (where classId != 4)
-- web
---- top 3 page: 19064 / 96602 = 19.73%
---- top 1 page: 12410 / 68115 = 18.22%
---- top 2 rows: 1563 / 11329 = 13.80%

-- mweb
---- top 3 page: 18226 / 91688 = 19.88%
---- top 1 page: 10319 / 57713 = 17.88%
---- top 2 rows: 878 / 6836 = 12.84%

-- boe
---- top 3 page: 27407 / 115798 = 23.67%
---- top 1 page: 14473 / 67524 = 21.43%
---- top 2 rows: 1641 / 9649 = 17.01%
