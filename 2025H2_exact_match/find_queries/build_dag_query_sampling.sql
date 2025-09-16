create or replace table `etsy-search-ml-dev.search.yzhang_emqueries_dag_base` as (
    -- get distinct 1000 most impressed queries every day
    with request_level_impression as (
        select
            query,
            visit_id,
            mmx_request_uuid,
            count(*) as per_request_impression
        from `etsy-data-warehouse-prod.rollups.search_impressions`
        where _date = date("2025-09-14")
        and query is not null and query != ""
        group by query, visit_id, mmx_request_uuid
    ),

    query_level_impression as (
        select query, sum(per_request_impression) as per_query_impression
        from request_level_impression
        group by query
    ),

    top_impressed_queries as (
        select query
        from query_level_impression
        order by per_query_impression desc
        limit 1000
    ),

    -- get requests for most impressed queries, take up to 3 per query
    rpc AS (
        SELECT distinct
            date(queryTime) as queryDate,
            request.query AS query,
            (SELECT NULLIF(query, '') FROM UNNEST(request.filter.query.translations) WHERE language = 'en') queryEn,
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
        WHERE date(queryTime) = date("2025-09-14")
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
            query,
            queryEn,
            mmxRequestUUID,
            platform,
            userLanguage,
            userCountry,
            userIdSeg,
            candidateSources,
            ROW_NUMBER() OVER (PARTITION BY queryDate, query ORDER BY RAND()) AS rn
        from rpc
        where query in (
            select query from top_impressed_queries
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
            mmxRequestUUID,
            platform,
            userLanguage,
            userCountry,
            userIdSeg,
            "Par3AboveExact3" as qlp_source,
            ARRAY(
                SELECT STRUCT(listing_id AS listingId, idx AS lastPassRank)
                FROM UNNEST(candidateSources) cs, UNNEST(cs.listingIds) AS listing_id WITH OFFSET idx
                WHERE cs.stage = "MO_LASTPASS"
                AND idx < 48
            ) listingSamples
        from sampled_requests
    )
    
    SELECT * EXCEPT (listingSamples)
    FROM sampled_qlp_raw, UNNEST(sampled_qlp_raw.listingSamples) listingSample
    order by queryDate, query, mmxRequestUUID, lastPassRank
)





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
  LEFT JOIN listing_description USING (listingId)
)