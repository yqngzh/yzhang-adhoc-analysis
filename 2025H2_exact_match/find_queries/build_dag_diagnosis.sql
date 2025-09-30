-- ======================== diagnosis-prod ====================================




-- ======================== diagnosis-dev ====================================
with tmp as (
  select distinct _date, mmxRequestUUID, query, queryEn, querySpellCorrect, listingId
  from `etsy-search-ml-dev.search.yzhang_emqueries_dag_sampling`
)
select _date, count(*) 
from tmp
group by _date
order by _date


with new_qlp as (
  select distinct query, listingId
  from `etsy-search-ml-dev.search.yzhang_emqueries_dag_sampling`
  where _date = "2025-09-22"
),
existing_qlp as (
  select distinct query, listingId
  from `etsy-search-ml-dev.search.yzhang_emqueries_dag_sampling`
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
                                                                    