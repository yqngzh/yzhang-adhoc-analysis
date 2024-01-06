---- Create query raw and processed mapping using the query_sessions_new table
create or replace table `etsy-sr-etl-prod.yzhang.query_raw_processed_mapping`
as (
    select distinct query as query_processed, query_raw
    from `etsy-data-warehouse-prod.search.query_sessions_new`
    where _date between date('2010-01-01') and date('2023-10-05')
    order by query_processed
)

select count(distinct query_raw) 
from `etsy-sr-etl-prod.yzhang.query_raw_processed_mapping`
where query_raw != query_processed
and query_raw is not null and query_processed is not null
-- query feature bank most recent: 1,988,039,065 queries
-- map table: 1,775,529,950 distinct raw queries, 1,125,797,568 raw queries differ from processed queries


---- Training data
-- get a day of FL data
create or replace table `etsy-sr-etl-prod.yzhang.query_missing_fl_web_1004`
as (
    select
        requestUUID, visitId,
        attributions,
        ctx.docInfo.queryInfo.*, 
        clientProvidedInfo.query.query as client_query,
        clientProvidedInfo.query.queryEn as client_queryEn,
        clientProvidedInfo.query.queryCorrected as client_queryCorrected,
    from `etsy-ml-systems-prod.attributed_instance.query_pipeline_web_organic_2023_10_04`, 
        unnest(contextualInfo) as ctx
)

-- number of requests (with purchase)
select count(distinct requestUUID)
from `etsy-sr-etl-prod.yzhang.query_missing_fl_web_1004`
where 'purchase' in unnest(attributions)
-- total 2270626 requests
-- 2265537 requests with clicks
-- 103951 requests with purchases

select count(distinct requestUUID)
from `etsy-sr-etl-prod.yzhang.query_missing_fl_web_1004`
where query is not null
and 'purchase' in unnest(attributions)
-- 2010366 requests have query value
-- 89886 requests with purchase have query value

-- 2270626 requests have client_query value; 30 requests with clent_query being empty string
-- 322512 requests have client_queryEn value - translation to english
-- 105368 requests have client_queryCorrected value
-- client_queryCorrected == client_query when it's not null
-- 34148 requests, client query != query (client query raw, query processed)
-- client query is raw, query is mixture of processed & raw

with query_raw_processed_mapping as (
    select query_processed, query_raw
    from `etsy-sr-etl-prod.yzhang.query_raw_processed_mapping`
    where query_processed != query_raw
)
select count(distinct requestUUID)
from `etsy-sr-etl-prod.yzhang.query_missing_fl_web_1004`
where query in (
    select query_raw from query_raw_processed_mapping
)
and query not in (
    select query_processed from query_raw_processed_mapping
)
and 'purchase' in unnest(attributions)
-- using ctx.docInfo.queryInfo.query
-- 325893 requests have raw queries that are not processed
-- 325893 / 2010366 (16%)
-- 325893 / 2270626 (14%)
-- 16255 requests with purchases have raw queries that are not processed
-- 16255 / 89886 (18%)
-- 16255 / 103951 (15%)
-- using client provided query
-- 1030457 requests have raw queries that are not processed
-- 50816 requests with purchases have raw queries that are not processed

-- query level metrics
-- allow list https://docs.etsycorp.com/fx-docs/docs/allowlist_md/query_pipeline_web_organic#querylevelmetricsfamily
with query_raw_processed_mapping as (
    select query_processed, query_raw
    from `etsy-sr-etl-prod.yzhang.query_raw_processed_mapping`
    where query_processed != query_raw
)
select count(distinct requestUUID)
from `etsy-sr-etl-prod.yzhang.query_missing_fl_web_1004`
where query is not null
and (
    queryLevelMetrics.bin is null and 
    queryLevelMetrics.cartRate is null and 
    queryLevelMetrics.clickRate is null and 
    queryLevelMetrics.isDigital is null and 
    queryLevelMetrics.purchaseEntropy is null and 
    queryLevelMetrics.purchaseRate is null and 
    queryLevelMetrics.totalPurchases is null
)
and 'purchase' in unnest(attributions)
and query in (
    select query_raw from query_raw_processed_mapping
)
and query not in (
    select query_processed from query_raw_processed_mapping
)
-- 325893 requests have query that is not processed
-- 350526 requests (out of 2010366, 17%) are missing all of queryLevelMetrics
-- out of them, 325704 (93%) have raw query that is not processed

-- 16255 requests with purchase have query that is not processed
-- 17721 requests with purchases (out of 89886, 20%) are missing all of queryLevelMetrics
-- out of them, 16251 (92%) have raw query that is not processed

-- trebuchet
-- allow list https://docs.etsycorp.com/fx-docs/docs/allowlist_md/query_pipeline_web_organic#trebuchetqueryfamily
with query_raw_processed_mapping as (
    select query_processed, query_raw
    from `etsy-sr-etl-prod.yzhang.query_raw_processed_mapping`
    where query_processed != query_raw
)
select count(distinct requestUUID)
from `etsy-sr-etl-prod.yzhang.query_missing_fl_web_1004`
where query is not null
and (
    trebuchetQuery.avgClickPrice is null and 
    trebuchetQuery.avgDwellTime is null and 
    trebuchetQuery.avgPurchasePrice is null and 
    trebuchetQuery.level2TaxoPath is null and 
    trebuchetQuery.logImpressionCount is null and 
    trebuchetQuery.logNoListingsDwelled is null and 
    trebuchetQuery.logTotalCarts is null and
    trebuchetQuery.logTotalCartsInVisit is null and
    trebuchetQuery.logTotalClicks is null and
    trebuchetQuery.logTotalClicksInVisit is null and
    trebuchetQuery.logTotalGmsInVisit is null and
    trebuchetQuery.logTotalPurchases is null and
    trebuchetQuery.logTotalPurchasesInVisit is null and
    trebuchetQuery.logTotalRevenueInVisit is null and
    trebuchetQuery.stdDwellTime is null and
    trebuchetQuery.stdPurchasePrice is null
)
and 'purchase' in unnest(attributions)
and query in (
    select query_raw from query_raw_processed_mapping
)
and query not in (
    select query_processed from query_raw_processed_mapping
)
select count(distinct requestUUID)
from `etsy-sr-etl-prod.yzhang.query_missing_fl_web_1004`
where query is not null
and (
    trebuchetQuery.avgClickPrice is null and 
    trebuchetQuery.avgDwellTime is null and 
    trebuchetQuery.avgPurchasePrice is null and 
    trebuchetQuery.level2TaxoPath is null and 
    trebuchetQuery.logImpressionCount is null and 
    trebuchetQuery.logNoListingsDwelled is null and 
    trebuchetQuery.logTotalCarts is null and
    trebuchetQuery.logTotalCartsInVisit is null and
    trebuchetQuery.logTotalClicks is null and
    trebuchetQuery.logTotalClicksInVisit is null and
    trebuchetQuery.logTotalGmsInVisit is null and
    trebuchetQuery.logTotalPurchases is null and
    trebuchetQuery.logTotalPurchasesInVisit is null and
    trebuchetQuery.logTotalRevenueInVisit is null and
    trebuchetQuery.stdDwellTime is null and
    trebuchetQuery.stdPurchasePrice is null
)
and 'purchase' in unnest(attributions)
and query in (
    select query_raw from query_raw_processed_mapping
)
and query not in (
    select query_processed from query_raw_processed_mapping
)

-- can these features be partially null? - yes for trebuchet


-- check feature logging raw data before joining
CREATE OR REPLACE EXTERNAL TABLE `etsy-sr-etl-prod.yzhang.query_missing_fl_raw`
OPTIONS (
    format = 'parquet',
    uris = ['gs://ml-systems-prod-raw-mmx-logs-zjh13h/java-consumer/parquet/query_pipeline_web_organic/_DATE=2023-10-04/_HOUR=23/*.parquet']
)

SELECT
  requestUUID,
  rankUnit.contextualInfo.array[SAFE_OFFSET(0)].docInfo.queryInfo.query,
  rankUnit.contextualInfo.array[SAFE_OFFSET(0)].docInfo.queryInfo.trebuchetQuery.avgClickPrice,
  rankUnit.contextualInfo.array[SAFE_OFFSET(0)].docInfo.queryInfo.trebuchetQuery.avgDwellTime,
  rankUnit.contextualInfo.array[SAFE_OFFSET(0)].docInfo.queryInfo.trebuchetQuery.avgPurchasePrice,
  rankUnit.contextualInfo.array[SAFE_OFFSET(0)].docInfo.queryInfo.trebuchetQuery.level2TaxoPath,
  rankUnit.contextualInfo.array[SAFE_OFFSET(0)].docInfo.queryInfo.trebuchetQuery.logImpressionCount,
  rankUnit.contextualInfo.array[SAFE_OFFSET(0)].docInfo.queryInfo.trebuchetQuery.logNoListingsDwelled,
  rankUnit.contextualInfo.array[SAFE_OFFSET(0)].docInfo.queryInfo.trebuchetQuery.logTotalCarts,
  rankUnit.contextualInfo.array[SAFE_OFFSET(0)].docInfo.queryInfo.trebuchetQuery.logTotalCartsInVisit,
  rankUnit.contextualInfo.array[SAFE_OFFSET(0)].docInfo.queryInfo.trebuchetQuery.logTotalClicks,
  rankUnit.contextualInfo.array[SAFE_OFFSET(0)].docInfo.queryInfo.trebuchetQuery.logTotalClicksInVisit,
  rankUnit.contextualInfo.array[SAFE_OFFSET(0)].docInfo.queryInfo.trebuchetQuery.logTotalGmsInVisit,
  rankUnit.contextualInfo.array[SAFE_OFFSET(0)].docInfo.queryInfo.trebuchetQuery.logTotalPurchases,
  rankUnit.contextualInfo.array[SAFE_OFFSET(0)].docInfo.queryInfo.trebuchetQuery.logTotalPurchasesInVisit,
  rankUnit.contextualInfo.array[SAFE_OFFSET(0)].docInfo.queryInfo.trebuchetQuery.logTotalRevenueInVisit,
  rankUnit.contextualInfo.array[SAFE_OFFSET(0)].docInfo.queryInfo.trebuchetQuery.stdDwellTime,
  rankUnit.contextualInfo.array[SAFE_OFFSET(0)].docInfo.queryInfo.trebuchetQuery.stdPurchasePrice,
FROM `etsy-sr-etl-prod.yzhang.query_missing_fl_raw` 
where rankUnit.contextualInfo.array[SAFE_OFFSET(0)].docInfo.queryInfo.query in ("gift", "gifts")
order by rankUnit.contextualInfo.array[SAFE_OFFSET(0)].docInfo.queryInfo.query
limit 100000


-- coverage in FBv2
with query_raw_processed_mapping as (
    select query_processed, query_raw
    from `etsy-sr-etl-prod.yzhang.query_raw_processed_mapping`
    where query_processed != query_raw
)
select count(key)
from `etsy-ml-systems-prod.feature_bank_v2.query_feature_bank_most_recent` fb
left join `etsy-data-warehouse-prod.rollups.query_level_metrics_raw` qlm
on fb.key = qlm.query_raw 
-- where (
--     trebuchetQuery_avgClickPrice is null and
--     trebuchetQuery_avgDwellTime is null and
--     trebuchetQuery_avgPurchasePrice is null and
--     trebuchetQuery_level2TaxoPath is null and
--     trebuchetQuery_logImpressionCount is null and
--     trebuchetQuery_logNoListingsDwelled is null and
--     trebuchetQuery_logTotalCarts is null and
--     trebuchetQuery_logTotalCartsInVisit is null and
--     trebuchetQuery_logTotalClicks is null and
--     trebuchetQuery_logTotalClicksInVisit is null and
--     trebuchetQuery_logTotalGmsInVisit is null and
--     trebuchetQuery_logTotalPurchases is null and
--     trebuchetQuery_logTotalPurchasesInVisit is null and
--     trebuchetQuery_logTotalRevenueInVisit is null and
--     trebuchetQuery_stdDwellTime is null and
--     trebuchetQuery_stdPurchasePrice is null
-- )
where (
    queryLevelMetrics_bin is null and 
    queryLevelMetrics_cartRate is null and 
    queryLevelMetrics_clickRate is null and 
    queryLevelMetrics_isDigital is null and 
    queryLevelMetrics_purchaseEntropy is null and 
    queryLevelMetrics_purchaseRate is null and 
    queryLevelMetrics_totalPurchases is null
)
and `key` in (
    select query_raw from query_raw_processed_mapping
)
and `key` not in (
    select query_processed from query_raw_processed_mapping
)
-- total queries: 1989642554
-- total query level gms: 214273836293

-- trebuchet
-- queries: 1297309599 (65%)
-- query level gms: 83390732041 (39%)
-- queries raw not processed: 452032438 (35% of queries all trebuchet null)
-- query level gms when query is raw not processed: 76869324791 (92% of trebuchet missing, 36% of total)

-- query level metrics
-- queries: 831182463 (41%)
-- query level gms: 93978173988 (44%)
-- queries raw not processed: 552176597 (66% of queries all qlm null)
-- query level gms when quer is raw not processed: 93885408879 (99.9%, 44% of total)
