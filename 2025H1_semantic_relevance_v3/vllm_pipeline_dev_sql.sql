CREATE OR REPLACE TABLE `etsy-search-ml-dev.search.yzhang_semrel_test` AS (
    WITH lfb AS (
        SELECT
            KEY listingId,
            (SELECT STRING_AGG(element, ', ') FROM UNNEST(descNgrams_ngrams.list)) listingDescNgrams,
            IFNULL(listingLlmFeatures_llmHeroImageDescription, "") listingHeroImageCaption,
        FROM `etsy-ml-systems-prod.feature_bank_v2.listing_feature_bank_most_recent`
    )
    SELECT * 
    FROM `etsy-data-warehouse-prod.search.sem_rel_hydrated_daily_requests_per_experiment`
    LEFT JOIN lfb USING (listingId)
    WHERE date BETWEEN DATE('2025-04-01') AND DATE('2025-04-10')
);



--- Sanity check pipeline implementation
WITH lfb AS (
    SELECT
        KEY listingId,
        (SELECT STRING_AGG(element, ', ') FROM UNNEST(descNgrams_ngrams.list)) listingDescNgrams,
        IFNULL(listingLlmFeatures_llmHeroImageDescription, "") listingHeroImageCaption,
    FROM `etsy-ml-systems-prod.feature_bank_v2.listing_feature_bank_2025-04-24`
), 
merged AS (
    SELECT *
    FROM `etsy-data-warehouse-prod.search.sem_rel_hydrated_daily_requests_per_experiment`
    -- FROM `etsy-data-warehouse-prod.search.sem_rel_hydrated_daily_requests`
    LEFT JOIN lfb USING (listingId)
    WHERE date = "2025-04-24"
)
SELECT count(*)
FROM merged
-- 13949394 rows, same as dataflow job
-- 181139 distinct guid

select count(*)
from `etsy-search-ml-dev.yzhang.semrel_llm_test_query_listing_metrics`
where date = "2025-04-24"
and modelName = "v3-finetuned-llama-8b"
-- 13949394 rows

select count(*)
from `etsy-search-ml-dev.yzhang.semrel_llm_test_request_metrics`
where date = "2025-04-24"
and modelName = "v3-finetuned-llama-8b"
-- 181138 rows


SELECT avg(relevanceNDCG4) 
FROM `etsy-data-warehouse-prod.search.sem_rel_requests_metrics_per_experiment` 
-- FROM  `etsy-search-ml-dev.yzhang.semrel_llm_test_request_metrics`
WHERE date = "2025-04-24" 



---------  FOR PAIRS TABLE
select date, count(*)
-- from `etsy-data-warehouse-prod.search.sem_rel_hydrated_daily_requests`
from `etsy-data-warehouse-prod.search.sem_rel_hydrated_daily_requests_per_experiment`
where date >= date("2025-05-10")
and guid is not null
group by date
order by date desc

select date, modelName, count(*)
-- from `etsy-data-warehouse-prod.search.sem_rel_query_listing_metrics`
from `etsy-data-warehouse-prod.search.sem_rel_query_listing_metrics_per_experiment`
where date >= date("2025-05-10")
group by date, modelName
order by date desc, modelName


-- DELETE FROM `etsy-data-warehouse-prod.search.sem_rel_query_listing_metrics`
DELETE FROM `etsy-data-warehouse-prod.search.sem_rel_query_listing_metrics_per_experiment`
WHERE date = date("2025-05-10") 
AND modelName = "v3-finetuned-llama-8b"

CREATE OR REPLACE VIEW `etsy-data-warehouse-prod.search.sem_rel_query_listing_metrics_vw` AS
-- CREATE OR REPLACE VIEW `etsy-data-warehouse-prod.search.sem_rel_query_listing_metrics_per_experiment_vw` AS
SELECT
    dr.date,
    ql.modelName,
    dr.guid,
    dr.visit_id,
    ql.resultType,
    dr.query,
    dr.listingId,
    dr.pageNum,
    CASE
        WHEN dr.retrievalRank IS NOT NULL THEN "pre-borda"
        WHEN dr.bordaRank IS NOT NULL THEN "post-borda"
        ELSE NULL
    END AS retrievalStage,
    ql.classId,
    ql.softmaxScores
-- FROM `etsy-data-warehouse-prod.search.sem_rel_hydrated_daily_requests` dr
-- JOIN `etsy-data-warehouse-prod.search.sem_rel_query_listing_metrics` ql
FROM `etsy-data-warehouse-prod.search.sem_rel_hydrated_daily_requests_per_experiment` dr
JOIN `etsy-data-warehouse-prod.search.sem_rel_query_listing_metrics_per_experiment` ql
    USING (date, tableUUID);



---------  FOR REQUEST TABLE
with reqs as (
    select distinct date, guid, resultType
    -- from `etsy-data-warehouse-prod.search.sem_rel_hydrated_daily_requests`
    from `etsy-data-warehouse-prod.search.sem_rel_hydrated_daily_requests_per_experiment`
    where date >= date("2025-05-10")
    and pageNum = 1
    and guid is not null
)
select date, count(*) from reqs
group by date
order by date desc

select date, modelName, count(*)
-- from `etsy-data-warehouse-prod.search.sem_rel_requests_metrics`
from `etsy-data-warehouse-prod.search.sem_rel_requests_metrics_per_experiment`
where date >= date("2025-05-10")
group by date, modelName
order by date desc, modelName


-- DELETE FROM `etsy-data-warehouse-prod.search.sem_rel_requests_metrics`
DELETE FROM `etsy-data-warehouse-prod.search.sem_rel_requests_metrics_per_experiment`
WHERE date = date("2025-04-29") 
AND modelName = "v3-finetuned-llama-8b"

CREATE OR REPLACE VIEW `etsy-data-warehouse-prod.search.sem_rel_requests_metrics_vw` AS
-- CREATE OR REPLACE VIEW `etsy-data-warehouse-prod.search.sem_rel_requests_metrics_per_experiment_vw` AS
SELECT DISTINCT
    rm.date,
    rm.modelName,
    rm.guid,
    rm.resultType,
    dr.query,
    rm.relevanceNDCG,
    rm.relevanceNDCG4,
    rm.relevanceNDCG10
-- FROM `etsy-data-warehouse-prod.search.sem_rel_hydrated_daily_requests` dr
-- JOIN `etsy-data-warehouse-prod.search.sem_rel_requests_metrics` ql
FROM `etsy-data-warehouse-prod.search.sem_rel_hydrated_daily_requests_per_experiment` dr
JOIN `etsy-data-warehouse-prod.search.sem_rel_requests_metrics_per_experiment` ql
    USING (date, guid);




-------------  To check backfill start date
with tmp as (
  select distinct date, guid, resultType
  from `etsy-data-warehouse-prod.search.sem_rel_hydrated_daily_requests_per_experiment`
  where date >= date("2025-04-10")
  and configFlag = "ranking/isearch.loc_no_xwalk_t2"
)
select date, count(*) 
from tmp
group by date
order by date desc
