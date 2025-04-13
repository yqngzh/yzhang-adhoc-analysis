CREATE OR REPLACE TABLE `etsy-search-ml-dev.search.yzhang_semrel_test` AS (
    SELECT * 
    FROM `etsy-data-warehouse-prod.search.sem_rel_hydrated_daily_requests_per_experiment`
    WHERE date BETWEEN DATE('2025-04-01') AND DATE('2025-04-10')
);