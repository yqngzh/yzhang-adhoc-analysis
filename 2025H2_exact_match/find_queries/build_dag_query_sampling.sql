create or replace table `etsy-search-ml-dev.search.yzhang_emqueries_dag_sampling` as (
    -- get distinct 100 most impressed queries every day
    with request_level_impression as (
        select
            query,
            _date,
            visit_id,
            mmx_request_uuid,
            count(*) as per_request_impression
        from `etsy-data-warehouse-prod.rollups.search_impressions`
        where _date between date("2025-09-19") and date("2025-09-21")
        and query is not null and query != ""
        group by query, _date, visit_id, mmx_request_uuid
    ),

    query_level_impression as (
        select query, _date, sum(per_request_impression) as per_query_impression
        from request_level_impression
        group by query, _date
    ),

    top_impressed_queries as (
        select query, _date
        from (
          select 
            query, _date, 
            ROW_NUMBER() OVER (PARTITION BY _date ORDER BY per_query_impression DESC) AS rn
          from query_level_impression
        )
        where rn <= 100
        order by _date, rn
    ),

    -- get requests for most impressed queries, take up to 3 per query
    rpc AS (
        SELECT distinct
            date(queryTime) as queryDate,
            request.query AS query,
            (SELECT NULLIF(query, '') FROM UNNEST(request.filter.query.translations) WHERE language = 'en') queryEn,
            NULLIF(response.spellCorrectionResult.correction, '') as querySpellCorrect,
            response.mmxRequestUUID,
            (SELECT
                CASE
                    WHEN value = 'web' THEN 'web'
                    WHEN value = 'web_mobile' THEN 'mweb'
                    WHEN value IN ('etsy_app_android', 'etsy_app_ios', 'etsy_app_other') THEN 'boe'
                    ELSE value
                END
                FROM unnest(request.context)
                WHERE key = "req_source"
            ) as platform,
            request.options.userLanguage userLanguage,
            request.options.userCountry userCountry,
            IF(
                request.options.personalizationOptions.userId > 0, 
                cast(request.options.personalizationOptions.userId as string), 
                "Signed_Out"
            ) as userIdSeg,
            OrganicRequestMetadata.candidateSources
        FROM `etsy-searchinfra-gke-prod-2.thrift_mmx_listingsv2search_search.rpc_logs_*`
        WHERE date(queryTime) between date("2025-09-19") and date("2025-09-21")
        AND request.options.cacheBucketId LIKE 'live%'
        AND request.query != ""
        AND request.options.csrOrganic
        AND request.options.searchPlacement IN ('wsg', "wmg", "allsr")
        AND request.offset = 0
        AND request.options.interleavingConfig IS NULL
        AND OrganicRequestMetadata IS NOT NULL
        AND EXISTS (
            SELECT 1 FROM UNNEST(OrganicRequestMetadata.candidateSources) cs
            WHERE cs.stage IN ('POST_FILTER', 'POST_SEM_REL_FILTER', 'POST_BORDA', 'RANKING', 'MO_LASTPASS')
                AND cs.listingIds IS NOT NULL
                AND ARRAY_LENGTH(cs.listingIds) > 0
        )
        AND NOT EXISTS (
            SELECT 1 FROM UNNEST(request.context)
            WHERE key = 'req_source' AND value = 'bot'
        )
        AND response.mmxRequestUUID IS NOT NULL
    ),

    rpc_top_impressed as (
        select 
            queryDate,
            rpc.query,
            queryEn,
            querySpellCorrect,
            mmxRequestUUID,
            platform,
            userLanguage,
            userCountry,
            userIdSeg,
            candidateSources,
            ROW_NUMBER() OVER (PARTITION BY queryDate, rpc.query ORDER BY RAND()) AS rn
        from rpc
        join top_impressed_queries ti
        on (
          rpc.query = ti.query
          and rpc.queryDate = ti._date
        )
    ),

    sampled_requests as (
        select * except (rn)
        from rpc_top_impressed
        where rn <= 3
    ),

    -- get listings
    sampled_qlp_raw as (
        select 
            queryDate,
            query,
            queryEn,
            querySpellCorrect,
            mmxRequestUUID,
            platform,
            userLanguage,
            userCountry,
            userIdSeg,
            "Par3AboveExact3" as qlpSource,
            ARRAY(
                SELECT STRUCT(
                  listing_id AS listingId,
                  cs.stage AS listingStage, 
                  idx AS listingRank
                )
                FROM UNNEST(candidateSources) cs, UNNEST(cs.listingIds) AS listing_id WITH OFFSET idx
                WHERE cs.stage = "MO_LASTPASS"
                AND idx < 24
            ) listingSamples
        from sampled_requests
    )
    
    SELECT
      GENERATE_UUID() AS tableUUID,
      mmxRequestUUID,
      queryDate as _date,
      query,
      listingId,
      ifnull(queryEn, "") queryEn,
      ifnull(querySpellCorrect, "") querySpellCorrect,
      queryDate,
      ifnull(platform, "") platform,
      ifnull(userLanguage, "") userLanguage,
      ifnull(userCountry, "") userCountry,
      ifnull(userIdSeg, "") userSegment,
      ifnull(listingStage, "") listingStage,
      ifnull(listingRank, -1) listingRank,
      qlpSource
    FROM sampled_qlp_raw, UNNEST(sampled_qlp_raw.listingSamples) listingSample
    order by queryDate, query, mmxRequestUUID, listingRank
)


-- stats
SELECT _date, count(*) 
FROM `etsy-search-ml-dev.search.yzhang_emqueries_dag_sampling` 
group by _date

with tmp as (
  select distinct _date, query, queryEn, querySpellCorrect, listingId
  from `etsy-search-ml-dev.search.yzhang_emqueries_dag_sampling`
)
select _date, count(*) 
from tmp
group by _date
order by _date

with new_qlp as (
  select distinct query, listingId
  from `etsy-search-ml-dev.search.yzhang_emqueries_dag_sampling`
  where _date = "2025-09-20"
),
existing_qlp as (
  select distinct query, listingId
  from `etsy-search-ml-dev.search.yzhang_emqueries_dag_sampling`
  where _date < "2025-09-20"
),
res as (
  SELECT new_qlp.*
  FROM new_qlp
  LEFT JOIN existing_qlp USING (query, listingId)
  WHERE existing_qlp.query IS NULL
  AND existing_qlp.listingId IS NULL
)
select count(*) from res

-- 2025-09-19	7200 request-qlp, 5981 qqenlp, 5875 qlp, 300 request-query 
-- 2025-09-20	7200 request-qlp, 5871 qqenlp, 5732 qlp, 300 request-query
                                  -- new qlp 4233
-- 2025-09-21	7200 request-qlp, 5782 qqenlp, 5595 qlp, 300 request-query
                                  -- new qlp 3495


delete from `etsy-search-ml-dev.search.yzhang_emqueries_dag_base_hydrated` 
where _date = "2025-09-17"

-- query, queryEn relationship
with tmp as (
  select distinct query, queryEn
  from `etsy-search-ml-dev.search.yzhang_emqueries_dag_base`
),
tmp2 as (
  select query, count(*) as cnt
  from tmp
  group by query
)
select * from tmp 
where query in (
  select query
  from tmp2 where cnt > 1
)
and lower(queryEn) != lower(query)
and queryEn != ""
order by query, queryEn


-- create initial base table
create or replace table `etsy-search-ml-dev.search.yzhang_emqueries_dag_base` as (
  WITH new_samples AS (
    SELECT *
    FROM `etsy-search-ml-dev.search.yzhang_emqueries_dag_sampling`
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
  select * from distinct_qlp
)





-- ======================== DEPRECATED ====================================
-- hydrated table schema
CREATE TABLE IF NOT EXISTS `{{ input_table }}_hydrated` (
    -- from base input table
    tableUUID  STRING  NOT NULL,
    _date  DATE  NOT NULL,
    query  STRING  NOT NULL, 
    queryEn  STRING,
    querySpellCorrect  STRING,
    queryDate  DATE,
    platform  STRING,
    userLanguage  STRING,
    userCountry  STRING,
    userSegment  STRING,
    listingId  INT64  NOT NULL,
    listingStage  STRING,
    listingRank  INT64,
    qlpSource  STRING  NOT NULL,
    -- query features
    queryBin  STRING,
    qisClass  STRING,
    queryRewrites  STRING,
    queryEntities  STRING,
    queryTaxoFullPath  STRING,
    queryTaxoTop  STRING,
    queryEntities_fandom  ARRAY<STRING>,
    queryEntities_motif  ARRAY<STRING>,
    queryEntities_style  ARRAY<STRING>,
    queryEntities_material  ARRAY<STRING>,
    queryEntities_color  ARRAY<STRING>,
    queryEntities_technique  ARRAY<STRING>,
    queryEntities_tangibleItem  ARRAY<STRING>,
    queryEntities_size  ARRAY<STRING>,
    queryEntities_occasion  ARRAY<STRING>,
    queryEntities_customization  ARRAY<STRING>,
    queryEntities_age  ARRAY<STRING>,
    queryEntities_price  ARRAY<STRING>,
    queryEntities_quantity  ARRAY<STRING>,
    queryEntities_recipient  ARRAY<STRING>,
    -- listing features
    listingCountry  STRING,
    shop_primaryLanguage  STRING,
    listingTitle  STRING,
    listingTitleEn  STRING,
    listingTaxo  STRING,
    listingTags  STRING,
    listingAttributes  STRING,
    listingShopName  STRING,
    listingDescription  STRING,
    listingDescriptionEn   STRING,
    listingDescNgrams   STRING,
    listingImageUrls  STRING,
    listingHeroImageCaption  STRING,
    listingVariations  STRING,
    listingReviews  STRING,
    listingEntities  STRING,
    listingEntities_tangibleItem  ARRAY<STRING>,
    listingEntities_material  ARRAY<STRING>,
    listingEntities_color  ARRAY<STRING>,
    listingEntities_style  ARRAY<STRING>,
    listingEntities_size  ARRAY<STRING>,
    listingEntities_occasion  ARRAY<STRING>,
    listingEntities_customization  ARRAY<STRING>,
    listingEntities_technique  ARRAY<STRING>,
    listingEntities_fandom  ARRAY<STRING>,
    listingEntities_brand  ARRAY<STRING>,
    listingEntities_quantity  ARRAY<STRING>,
    listingEntities_recipient  ARRAY<STRING>,
    listingEntities_age  ARRAY<STRING>,
    listingEntities_misc  ARRAY<STRING>
);


-- ============================================================
-- 2. Query hydration (only queries from base table)
-- ============================================================
create or replace table `etsy-search-ml-dev.search.yzhang_emqueries_dag_qfs` as (
    WITH ids AS (
        SELECT DISTINCT query
        FROM `etsy-search-ml-dev.search.yzhang_emqueries_dag_base`
    ),

    qlm AS (
      select distinct query_raw as query, bin as queryBin 
      from `etsy-data-warehouse-prod.rollups.query_level_metrics_raw`
    ),

    qisv3 AS (
      select query_raw query,
      CASE 
        WHEN prediction = 0 THEN 'broad' 
        WHEN prediction = 1 THEN 'direct_unspecified'
        WHEN prediction = 2 THEN 'direct_specified' 
      END as qisClass
      from `etsy-search-ml-prod.mission_understanding.qis_scores_v3`
    ),

    query_rewrites AS (
        SELECT key AS query, STRING_AGG(unnested_value, ", ") AS queryRewrites
        FROM `etsy-search-ml-dev.mission_understanding.smu_query_rewriting_v2_dpo_semrel`,
            UNNEST(value) AS unnested_value
        WHERE key IN (SELECT query FROM ids)
        GROUP BY key
    ),

    query_entities AS (
        SELECT
            searchQuery AS query,
            ANY_VALUE(entities) AS queryEntities
        FROM `etsy-data-warehouse-prod.arizona.query_entity_features`
        WHERE searchQuery IN (SELECT query FROM ids)
        GROUP BY query
    ),

    qtcv5 AS (
        SELECT DISTINCT
            COALESCE(s.query, b.query) AS query,
            COALESCE(s.full_path, b.full_path) AS queryTaxoFullPath
        FROM `etsy-data-warehouse-prod.mission_understanding.query_taxonomy_seller` s
        FULL OUTER JOIN `etsy-data-warehouse-prod.mission_understanding.query_taxonomy_buyer` b
        USING(query)
        WHERE COALESCE(s.query, b.query) IN (SELECT query FROM ids)
    ),

    qee AS (
        SELECT
            searchQuery AS query,
            ANY_VALUE(fandom)        AS queryEntities_fandom,
            ANY_VALUE(motif)         AS queryEntities_motif,
            ANY_VALUE(style)         AS queryEntities_style,
            ANY_VALUE(material)      AS queryEntities_material,
            ANY_VALUE(color)         AS queryEntities_color,
            ANY_VALUE(technique)     AS queryEntities_technique,
            ANY_VALUE(tangibleItem)  AS queryEntities_tangibleItem,
            ANY_VALUE(size)          AS queryEntities_size,
            ANY_VALUE(occasion)      AS queryEntities_occasion,
            ANY_VALUE(customization) AS queryEntities_customization,
            ANY_VALUE(age)           AS queryEntities_age,
            ANY_VALUE(price)         AS queryEntities_price,
            ANY_VALUE(quantity)      AS queryEntities_quantity,
            ANY_VALUE(recipient)     AS queryEntities_recipient
        FROM `etsy-search-ml-prod.mission_understanding.query_entity_features`
        WHERE searchQuery IN (SELECT query FROM ids)
        GROUP BY query
    )

    SELECT
        b.*,
        queryBin,
        qisClass,
        queryRewrites,
        queryEntities,
        queryTaxoFullPath,
        SPLIT(queryTaxoFullPath, ".")[OFFSET(0)] AS queryTaxoTop,
        queryEntities_fandom,
        queryEntities_motif,
        queryEntities_style,
        queryEntities_material,
        queryEntities_color,
        queryEntities_technique,
        queryEntities_tangibleItem,
        queryEntities_size,
        queryEntities_occasion,
        queryEntities_customization,
        queryEntities_age,
        queryEntities_price,
        queryEntities_quantity,
        queryEntities_recipient
    FROM `etsy-search-ml-dev.search.yzhang_emqueries_dag_base` b
    LEFT JOIN qlm USING (query)
    LEFT JOIN qisv3 USING (query)
    LEFT JOIN query_rewrites USING (query)
    LEFT JOIN query_entities USING (query)
    LEFT JOIN qtcv5 USING (query)
    LEFT JOIN qee USING (query)
)


-- ============================================================
-- 3. Listing hydration (only listingIds from base table)
-- ============================================================
CREATE OR REPLACE TABLE `etsy-search-ml-dev.search.yzhang_emqueries_dag_hydrated` AS (
  WITH ids AS (
    SELECT DISTINCT listingId
    FROM `etsy-search-ml-dev.search.yzhang_emqueries_dag_base`
  ),

  lfb AS (
    SELECT 
      key AS listingId,
      COALESCE(verticaListings_title, verticaListingTranslations_primaryLanguageTitle, "") AS listingTitle,
      IFNULL(verticaListingTranslations_machineTranslatedEnglishTitle, "") AS listingTitleEn,
      IFNULL(verticaListings_taxonomyPath, "") AS listingTaxo,
      IFNULL(verticaListings_tags, "") AS listingTags,
      (SELECT STRING_AGG(element, ';') FROM UNNEST(kbAttributesV2_sellerAttributesV2.list)) AS listingAttributes,
      IFNULL(verticaSellerBasics_shopName, "") AS listingShopName,
      (SELECT STRING_AGG(element, ', ') FROM UNNEST(descNgrams_ngrams.list)) AS listingDescNgrams,
      IFNULL(listingLlmFeatures_llmHeroImageDescription, "") AS listingHeroImageCaption,
      IFNULL(verticaShopSettings_primaryLanguage, "") AS shop_primaryLanguage,
      IFNULL(localeFeatures_listingCountry, "") AS listingCountry
    FROM `etsy-ml-systems-prod.feature_bank_v2.listing_feature_bank_most_recent`
    WHERE key IN (SELECT listingId FROM ids)
  ),

  listing_entities AS (
    SELECT
      listing_id AS listingId,
      tangible_item AS listingEntities_tangibleItem,
      material      AS listingEntities_material,
      color         AS listingEntities_color,
      style         AS listingEntities_style,
      size          AS listingEntities_size,
      occasion      AS listingEntities_occasion,
      customization AS listingEntities_customization,
      technique     AS listingEntities_technique,
      fandom        AS listingEntities_fandom,
      brand         AS listingEntities_brand,
      quantity      AS listingEntities_quantity,
      recipient     AS listingEntities_recipient,
      age           AS listingEntities_age,
      misc          AS listingEntities_misc
    FROM `etsy-data-warehouse-prod.inventory_ml.listing_entities_raw_v1`
    WHERE listing_id IN (SELECT listingId FROM ids)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY listing_id ORDER BY processing_timestamp DESC) = 1
  ),

  listing_entities_json AS (
    SELECT
      listing_id AS listingId,
      TO_JSON_STRING(STRUCT(
        IFNULL(tangible_item, ARRAY<STRING>[]) AS tangibleItem,
        IFNULL(material, ARRAY<STRING>[])     AS material,
        IFNULL(color, ARRAY<STRING>[])        AS color,
        IFNULL(style, ARRAY<STRING>[])        AS style,
        IFNULL(size, ARRAY<STRING>[])         AS size,
        IFNULL(occasion, ARRAY<STRING>[])     AS occasion,
        IFNULL(customization, ARRAY<STRING>[]) AS customization,
        IFNULL(technique, ARRAY<STRING>[])    AS technique,
        IFNULL(fandom, ARRAY<STRING>[])       AS fandom,
        IFNULL(brand, ARRAY<STRING>[])        AS brand,
        IFNULL(quantity, ARRAY<STRING>[])     AS quantity,
        IFNULL(recipient, ARRAY<STRING>[])    AS recipient,
        IFNULL(age, ARRAY<STRING>[])          AS age,
        IFNULL(misc, ARRAY<STRING>[])         AS misc
      )) AS listingEntities
    FROM `etsy-data-warehouse-prod.inventory_ml.listing_entities_raw_v1`
    WHERE listing_id IN (SELECT listingId FROM ids)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY listing_id ORDER BY processing_timestamp DESC) = 1
  ),

  listing_description AS (
    SELECT 
      listing_id AS listingId, 
      description AS listingDescriptionEn
    FROM `etsy-data-warehouse-prod.rollups.listing_full_descriptions`
    WHERE listing_id IN (SELECT listingId FROM ids)
  ),

  listing_variations AS (
    SELECT 
      listing_id AS listingId,
      STRING_AGG(CONCAT(attribute_name, ': ', attribute_value), '; ') AS listingVariations
    FROM `etsy-data-warehouse-prod.listing_mart.listing_variation_attributes`
    WHERE listing_id IN (SELECT listingId FROM ids)
    GROUP BY listing_id
  ),

  listing_reviews AS (
    SELECT
      listing_id AS listingId,
      ARRAY_TO_STRING(ARRAY_AGG(review ORDER BY update_date DESC LIMIT 5), ' | ') AS listingReviews
    FROM `etsy-data-warehouse-prod.etsy_shard.listing_review`
    WHERE review IS NOT NULL AND review != ""
      AND listing_id IN (SELECT listingId FROM ids)
    GROUP BY listing_id
  ),

  listing_images AS (
    SELECT 
      listing_id AS listingId,
      STRING_AGG(url, ';' ORDER BY img_rank ASC) AS listingImageUrls
    FROM `etsy-data-warehouse-prod.computer_vision.listing_image_paths`
    WHERE listing_id IN (SELECT listingId FROM ids)
    GROUP BY listing_id
  )

  SELECT 
    hq.*,
    listingCountry,
    shop_primaryLanguage,
    listingTitle,
    listingTitleEn,
    listingTaxo,
    listingTags,
    listingAttributes,
    listingShopName,
    "" AS listingDescription,
    listingDescriptionEn,
    listingDescNgrams,
    listingImageUrls,
    listingHeroImageCaption,
    listingVariations,
    listingReviews,
    listingEntities,
    listingEntities_tangibleItem,
    listingEntities_material,
    listingEntities_color,
    listingEntities_style,
    listingEntities_size,
    listingEntities_occasion,
    listingEntities_customization,
    listingEntities_technique,
    listingEntities_fandom,
    listingEntities_brand,
    listingEntities_quantity,
    listingEntities_recipient,
    listingEntities_age,
    listingEntities_misc
  FROM `etsy-search-ml-dev.search.yzhang_emqueries_dag_qfs` hq
  LEFT JOIN lfb USING (listingId)
  LEFT JOIN listing_variations USING (listingId)
  LEFT JOIN listing_reviews USING (listingId)
  LEFT JOIN listing_images USING (listingId)
  LEFT JOIN listing_entities USING (listingId)
  LEFT JOIN listing_entities_json USING (listingId)
  LEFT JOIN listing_description USING (listingId)
)

