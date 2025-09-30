-- analysis code for the first batch
-- https://github.com/yqngzh/yzhang-adhoc-analysis/blob/master/2025H2_exact_match/find_queries/explore2_get_impressed_page1.sql


-- =================================================
-- _sampling
-- =================================================
CREATE OR REPLACE TABLE `etsy-search-ml-dev.search.sem_rel_labels_sampling`
PARTITION BY _date
AS 
SELECT 
    GENERATE_UUID() AS tableUUID,
    mmxRequestUUID,
    DATE("2025-09-19") AS _date, -- date of announcing this 99 queries
    query,
    listingId,
    IFNULL(queryEn, "") queryEn,
    "" AS querySpellCorrect,
    queryDate,
    IFNULL(platform, "") platform,
    IFNULL(userLanguage, "") userLanguage,
    IFNULL(userCountry, "") userCountry,
    IFNULL(si_so, "") userId,
    "MO_LASTPASS" AS listingStage,
    IFNULL(listingRank, -1) listingRank,
    "Par3AboveExact3At24" AS qlpSource
FROM `etsy-search-ml-dev.search.yzhang_emqueries_issue_base`


-- =================================================
-- _base
-- =================================================
CREATE OR REPLACE TABLE `etsy-search-ml-dev.search.sem_rel_labels_base`
PARTITION BY _date
AS (
  WITH new_samples AS (
    SELECT *
    FROM `etsy-search-ml-dev.search.sem_rel_labels_sampling`
    WHERE _date = "2025-09-19"
  ),
  -- find distinct query listing pairs in nes sample
  distinct_qqqlp AS (
      SELECT DISTINCT _date, query, listingId, queryEn, querySpellCorrect
      FROM new_samples
  ),
  -- queryEn and querySpellCorrect are not consistent across requests
  -- prioritize rows with these info
  distinct_qlp AS (
      SELECT _date, query, listingId, queryEn, querySpellCorrect
      FROM (
          SELECT
              *,
              ROW_NUMBER() OVER (
                  PARTITION BY query, listingId
                  ORDER BY
                      CASE WHEN queryEn IS NOT NULL AND queryEn <> '' THEN 1 ELSE 0 END DESC,
                      CASE WHEN querySpellCorrect IS NOT NULL AND querySpellCorrect <> '' THEN 1 ELSE 0 END DESC
              ) AS rn
          FROM distinct_qqqlp
          QUALIFY rn = 1
      )
  )
  SELECT * FROM distinct_qlp
)


-- =================================================
-- _llm
-- =================================================
CREATE OR REPLACE TABLE `etsy-search-ml-dev.search.sem_rel_labels_llm`
PARTITION BY _date
AS (
    WITH tmp AS (
        SELECT 
            *, 
            ROW_NUMBER() OVER(PARTITION BY query, listingId ORDER BY RAND()) AS rn
        FROM `etsy-search-ml-dev.search.yzhang_emqueries_issue_llm`
    )
    SELECT DISTINCT
        DATE("2025-09-19") AS _date,
        query,
        listingId,
        "o3_s3h7fs3" AS llm_name,
        llm_final_label,
        llm_consensus_type,
        "" AS llm_reason,
    FROM tmp
    WHERE rn = 1
)


-- =================================================
-- _human_sampling
-- =================================================
CREATE OR REPLACE TABLE `etsy-search-ml-dev.search.sem_rel_labels_human_sampling`
PARTITION BY _date
AS (
    WITH tmp AS (
        SELECT
            DATE("2025-09-19") AS _date,
            *,
            "" AS listingShopName,
            "Par3AboveExact3At24" AS qlpSource
        FROM `etsy-search-ml-dev.search.exact_match_key_queries_for_v4_2`
    )
    SELECT * EXCEPT (tableUUID)
    FROM tmp    
)


-- -- =================================================
-- -- _human_base
-- -- =================================================
-- CREATE OR REPLACE TABLE `etsy-search-ml-dev.search.sem_rel_labels_human_base`
-- PARTITION BY _date
-- AS (
--     SELECT * FROM `etsy-search-ml-dev.search.sem_rel_labels_human_sampling`
-- )


-- =================================================
-- copy to prod
-- =================================================
CREATE OR REPLACE TABLE `etsy-search-ml-prod.search.sem_rel_labels_sampling`
PARTITION BY _date
AS (
    SELECT * FROM `etsy-search-ml-dev.search.sem_rel_labels_sampling`
)
