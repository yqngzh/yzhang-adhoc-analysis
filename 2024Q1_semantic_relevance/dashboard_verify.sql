select count(distinct etsyUUID)
from `etsy-data-warehouse-prod.search.sem_rel_hydrated_daily_requests`

with tmp as (
    select distinct query, listingId
    from `etsy-data-warehouse-prod.search.sem_rel_hydrated_daily_requests`
    where platform = 'boe'
)
select count(*) from tmp

select count(distinct query)
from `etsy-data-warehouse-prod.search.sem_rel_hydrated_daily_requests`

with tmp as (
    select distinct query, queryBin
    from `etsy-data-warehouse-prod.search.sem_rel_hydrated_daily_requests`
    where platform in ('web', 'mweb')
)
select queryBin, count(*) as numQuery
from tmp
group by queryBin
order by queryBin

with tmp as (
    select distinct etsyUUID, platform
    from `etsy-data-warehouse-prod.search.sem_rel_hydrated_daily_requests`
)
select platform, count(*) as numRequests
from tmp
group by platform



with tmp as (
    select distinct query, queryBin
    from `etsy-data-warehouse-prod.search.sem_rel_hydrated_daily_requests`
    where platform in ('web', 'mweb')
    and date = date('2024-05-01')
)
select queryBin, count(*) as numQuery
from tmp
group by queryBin
order by queryBin