--- look for partial relevance examples 
with qis as (
  SELECT distinct query_raw 
  FROM `etsy-search-ml-dev.mission_understanding.qis_scores`
  where class_id = 2
),
qbin as (
  select key as query_raw
  from `etsy-ml-systems-prod.feature_bank_v2.query_feature_bank_most_recent`
  where queryLevelMetrics_bin like "top%"
)
select query_raw
from qis 
join qbin
using (query_raw)
where rand() > 0.9
limit 500


--- V2 representative
SELECT count(distinct query) 
FROM `etsy-sr-etl-prod.yzhang.sem_rel_human_annotation_v2`


with ql_v2 as (
    select distinct query, listingId
    from `etsy-sr-etl-prod.yzhang.sem_rel_human_annotation_v2`
),
n_listing_per_query as (
    select query, count(*) as n_listing
    from ql_v2
    group by query
),
n_query_per_listing as (
    select listingId, count(*) as n_query
    from ql_v2
    group by listingId
)
select avg(n_listing) from n_listing_per_query


with queries_with_gms as (
    select query_raw, bin, purchase_rate, gms
    from `etsy-data-warehouse-prod.rollups.query_level_metrics_raw`
    where gms > 0
),
gms_quantiles as (
    select
        percentiles[offset(50)] as p50,
        percentiles[offset(75)] as p75,
        percentiles[offset(90)] as p90,
        percentiles[offset(95)] as p95,
        percentiles[offset(99)] as p99,
    from (
        select approx_quantiles(gms, 100) percentiles
        from queries_with_gms
    )
),
top_queries as (
    select query_raw, bin, purchase_rate, gms
    from queries_with_gms
    where gms >= (select p95 from gms_quantiles)
    and gms < (select p99 from gms_quantiles)
)
select count(distinct query_raw)
from top_queries
where query_raw in (
    select query
    from `etsy-sr-etl-prod.yzhang.sem_rel_human_annotation_v2`
) 
-- above 1%   7420 / 337799   = 2.2%
-- 1% - 5%    2648 / 1337153  = 0.2%
-- 5% - 10%   1034 / 1679346  = 0.06%
-- 10% - 25%  1276 / 5031359  = 0.03%
-- 25% - 50%  852  / 8373515  = 0.01%
-- 50% - 100% 860  / 16746293 = 0.005%


with bin_queries as (
    select distinct query_raw
    from `etsy-data-warehouse-prod.rollups.query_level_metrics_raw`
    where bin = 'top.01'
)
select count(distinct query_raw)
from bin_queries
where query_raw in (
    select query
    from `etsy-sr-etl-prod.yzhang.sem_rel_human_annotation_v2`
) 
-- top.01  5734 / 80549
-- top.1   4500 / 724941
-- head    6373 / 31413695
-- torso   2678 / 209420816
-- tail    2354 / 563812666


with qis_queries as (
    select distinct query
    from `etsy-search-ml-dev.mission_understanding.qis_scores_v2`
    where prediction = 0
)
select count(distinct query)
from qis_queries
where query in (
    select query
    from `etsy-sr-etl-prod.yzhang.sem_rel_human_annotation_v2`
) 
-- broad                1326 / 110028 = 1.2%
-- direct unspecified   2217 / 134459 = 1.6%
-- direct specified     2431 / 265513 = 0.9% 


with query_taxo as (
    select 
        key as query, 
        queryTaxoClassification_taxoPath as query_taxo,
        split(queryTaxoClassification_taxoPath, '.')[safe_offset(0)] as level1_query_taxo, 
    from `etsy-ml-systems-prod.feature_bank_v2.query_feature_bank_most_recent`
    where queryTaxoClassification_taxoPath is not null 
    -- 8% queries have value
)
-- select level1_query_taxo, count(*) as cnt
-- from query_taxo
-- group by level1_query_taxo
-- order by cnt desc
select count(distinct query)
from query_taxo
where query in (
    select query
    from `etsy-sr-etl-prod.yzhang.sem_rel_human_annotation_v2`
)
and level1_query_taxo = 'home_and_living'


with listing_with_gms as (
    select listing_id, past_year_gms 
    from `etsy-data-warehouse-prod.rollups.active_listing_basics`
    where past_year_gms > 0
    -- 26% listings
),
gms_quantiles as (
    select
        percentiles[offset(50)] as p50,
        percentiles[offset(75)] as p75,
        percentiles[offset(90)] as p90,
        percentiles[offset(95)] as p95,
        percentiles[offset(99)] as p99,
    from (
        select approx_quantiles(past_year_gms, 100) percentiles
        from listing_with_gms
    )
),
top_listings as (
    select listing_id, past_year_gms
    from listing_with_gms
    where past_year_gms >= (select p95 from gms_quantiles)
    and past_year_gms < (select p99 from gms_quantiles)
)
select count(distinct listing_id)
from top_listings
where listing_id in (
    select listing_id
    from `etsy-sr-etl-prod.yzhang.sem_rel_human_annotation_v2`
) 
-- above 1%   4538  /  331618   = 1.37%
-- 1% - 5%    3856  /  1330851  = 0.29%
-- 5% - 10%   2194  /  1661593  = 0.13%
-- 10% - 25%  3177  /  4986729  = 0.06%
-- 25% - 50%  2236  /  8310390  = 0.03%
-- 50% - 100% 1757  /  16619692 = 0.01%


with listing_taxo as (
    select 
        listing_id, top_category
    from `etsy-data-warehouse-prod.rollups.active_listing_basics`
    where top_category is not null 
    -- 99.99% listings have value
)
-- select top_category, count(*) as cnt
-- from listing_taxo
-- group by top_category
-- order by cnt desc
select count(distinct listing_id)
from listing_taxo
where listing_id in (
    select listingId
    from `etsy-sr-etl-prod.yzhang.sem_rel_human_annotation_v2`
)
and top_category = 'home_and_living'
