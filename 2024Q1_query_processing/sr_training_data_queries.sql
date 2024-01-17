select
    requestUUID, ctx.docInfo.queryInfo.query, ctx.id
from `etsy-ml-systems-prod.attributed_instance.query_pipeline_web_organic_2024_01_04`, unnest(contextualInfo) as ctx
  where ctx.docInfo.queryInfo.query != ctx.id
-- empty; context query is ID

-- look at FL data
CREATE OR REPLACE EXTERNAL TABLE `etsy-sr-etl-prod.yzhang.fl_240103`
OPTIONS (
    format = 'parquet',
    uris = ['gs://etldata-prod-search-ranking-data-hkwv8r/feature_logging_training_data_parquet/query_pipeline_web_organic/tight_purchase/_DATE=2024-01-03/results/*.parquet']
)

--- use AttributedInstance, filter to purchase requests
create or replace table `etsy-sr-etl-prod.yzhang.attributed_instance_queries_1229_0104` as (
  with data_0104 as (
    select distinct requestUUID, ctx.docInfo.queryInfo.query as context_query, clientProvidedInfo.query.query as client_query, "2024-01-04" as data_date
    from `etsy-ml-systems-prod.attributed_instance.query_pipeline_web_organic_2024_01_04`, unnest(contextualInfo) as ctx
    where "purchase" in unnest(attributions)
    and userCountry = 'US'
  ),
  data_0103 as (
    select distinct requestUUID, ctx.docInfo.queryInfo.query as context_query, clientProvidedInfo.query.query as client_query, "2024-01-03" as data_date
    from `etsy-ml-systems-prod.attributed_instance.query_pipeline_web_organic_2024_01_03`, unnest(contextualInfo) as ctx
    where "purchase" in unnest(attributions)
    and userCountry = 'US'
  ),
  data_0102 as (
    select distinct requestUUID, ctx.docInfo.queryInfo.query as context_query, clientProvidedInfo.query.query as client_query, "2024-01-02" as data_date
    from `etsy-ml-systems-prod.attributed_instance.query_pipeline_web_organic_2024_01_02`, unnest(contextualInfo) as ctx
    where "purchase" in unnest(attributions)
    and userCountry = 'US'
  ),
  data_0101 as (
    select distinct requestUUID, ctx.docInfo.queryInfo.query as context_query, clientProvidedInfo.query.query as client_query, "2024-01-01" as data_date
    from `etsy-ml-systems-prod.attributed_instance.query_pipeline_web_organic_2024_01_01`, unnest(contextualInfo) as ctx
    where "purchase" in unnest(attributions)
    and userCountry = 'US'
  ),
  data_1231 as (
    select distinct requestUUID, ctx.docInfo.queryInfo.query as context_query, clientProvidedInfo.query.query as client_query, "2023-12-31" as data_date
    from `etsy-ml-systems-prod.attributed_instance.query_pipeline_web_organic_2023_12_31`, unnest(contextualInfo) as ctx
    where "purchase" in unnest(attributions)
    and userCountry = 'US'
  ),
  data_1230 as (
    select distinct requestUUID, ctx.docInfo.queryInfo.query as context_query, clientProvidedInfo.query.query as client_query, "2023-12-30" as data_date
    from `etsy-ml-systems-prod.attributed_instance.query_pipeline_web_organic_2023_12_30`, unnest(contextualInfo) as ctx
    where "purchase" in unnest(attributions)
    and userCountry = 'US'
  ),
  data_1229 as (
    select distinct requestUUID, ctx.docInfo.queryInfo.query as context_query, clientProvidedInfo.query.query as client_query, "2023-12-29" as data_date
    from `etsy-ml-systems-prod.attributed_instance.query_pipeline_web_organic_2023_12_29`, unnest(contextualInfo) as ctx
    where "purchase" in unnest(attributions)
    and userCountry = 'US'
  )
  select * from data_1229
  union all
  select * from data_1230
  union all
  select * from data_1231
  union all
  select * from data_0101
  union all
  select * from data_0102
  union all
  select * from data_0103
  union all
  select * from data_0104
  order by data_date, requestUUID
)

with requests_by_date as (
  select distinct data_date, requestUUID
  from `etsy-sr-etl-prod.yzhang.attributed_instance_queries_1229_0104`
)
select data_date, count(*)
from requests_by_date
group by data_date
-- 49524-62348, less than json / parquet data

select count(*) from `etsy-sr-etl-prod.yzhang.attributed_instance_queries_1229_0104`
where client_query is null
-- 0; there's always client query

with requests_with_raw_query as (
  select distinct data_date, requestUUID, client_query
  from `etsy-sr-etl-prod.yzhang.attributed_instance_queries_1229_0104`
)
select count(*) from requests_with_raw_query
-- 407397 rows & distinct requests
