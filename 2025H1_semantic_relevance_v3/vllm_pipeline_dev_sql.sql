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
