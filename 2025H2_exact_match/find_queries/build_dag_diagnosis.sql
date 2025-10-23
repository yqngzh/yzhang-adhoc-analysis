-- ======================== diagnosis-prod ====================================
select _date, count(*) 
from `etsy-search-ml-prod.search.sem_rel_labels_sampling`
group by _date
order by _date

with tmp as (
  select distinct _date, query, listingId
  from `etsy-search-ml-prod.search.sem_rel_labels_human_sampling`
)
select _date, query, count(*) cnt
from tmp 
group by _date, query
order by _date desc, cnt desc

with distinct_qlp as (
  select distinct query, listingId 
  from `etsy-search-ml-prod.search.sem_rel_labels_sampling`
  where _date = "2025-10-20"
),
existing_qlp as (
  select distinct query, listingId, 1 as seen 
  from `etsy-search-ml-prod.search.sem_rel_labels_sampling`
  where _date < "2025-10-20"
),
tmp as (
  select * 
  from existing_qlp
  join distinct_qlp using (query, listingId)
  -- SELECT d.*
  -- FROM distinct_qlp d
  -- LEFT JOIN existing_qlp e USING (query, listingId)
  -- WHERE e.seen IS NULL
)
-- select count(*) from distinct_qlp
select count(*) from tmp


-- sampling
---- 2025-10-20	5837 - distinct qlp 4464, seen 537
---- 2025-10-21	5664 - distinct qlp 4335, seen 828
---- 2025-10-22	5400 - distinct qlp 4260, seen 1156
-- base
---- 2025-10-20	3927
---- 2025-10-21	3507
---- 2025-10-22	3104
-- human sampling
---- 2025-10-20	75
---- 2025-10-21	129
---- 2025-10-22	79
-- human base
---- 2025-10-20	75
---- 2025-10-21	129
---- 2025-10-22	78




-- ======================== diagnosis-dev ====================================
with tmp as (
  select distinct _date, mmxRequestUUID, query, queryEn, querySpellCorrect, listingId
  from `etsy-search-ml-prod.search.yzhang_emqueries_dag_sampling`
)
select _date, count(*) 
from tmp
group by _date
order by _date


with new_qlp as (
  select distinct query, listingId
  from `etsy-search-ml-prod.search.yzhang_emqueries_dag_sampling`
  where _date = "2025-09-22"
),
existing_qlp as (
  select distinct query, listingId
  from `etsy-search-ml-prod.search.yzhang_emqueries_dag_sampling`
  where _date < "2025-09-22"
),
res as (
  SELECT new_qlp.*
  FROM new_qlp
  LEFT JOIN existing_qlp USING (query, listingId)
  WHERE existing_qlp.query IS NULL
  AND existing_qlp.listingId IS NULL
)
select count(*) from res

-- 2025-09-19	7200 request-qlp,  100 query,  300  request-query   5981  qqenlp, 5875  qlp
-- 2025-09-20	7200 request-qlp,  100 query,  300  request-query,  5871  qqenlp, 5732  qlp 
                                                                               -- 4233  new qlp
-- 2025-09-21	7200 request-qlp,  100 query,  300  request-query,  5782  qqenlp, 5595  qlp 
                                                                               -- 3495  new qlp
-- 2025-09-22   55236 request-qlp, 1000 query, 2304 request-query,  41975 qqenlp, 41209 qlp
                                                                               -- 38236 new qlp
                                                                    