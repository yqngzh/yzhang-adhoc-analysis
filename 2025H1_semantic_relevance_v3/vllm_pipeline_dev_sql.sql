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