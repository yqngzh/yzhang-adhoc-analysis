with sampled as (
  select query_raw
  from `etsy-data-warehouse-prod.search.query_bins`
  where (
    bin is NULL
    or bin in ('head', 'torso', 'tail')
  )
  and rand() < 0.01
)
select * from sampled
limit 500
