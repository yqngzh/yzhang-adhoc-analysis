declare start_date date default "2025-08-01";
declare end_date date default "2025-08-26";

-- ============================================================
-- 0. Base sampling
-- ============================================================
create or replace table `etsy-search-ml-dev.search.yzhang_emqueries_aug_base_raw` as (
    with semrel_teacher_page1 as (
        SELECT
            mmxRequestUUID,
            guid,
            query,
            r.queryBin,
            r.qisClass,
            listingId,
            r.date,
            platform,
            userLanguage,
            userCountry,
            CASE 
                WHEN classId = 1 THEN 'not_relevant' 
                WHEN classId = 2 THEN 'partial'
                WHEN classId = 3 THEN 'relevant' 
            END AS semrelClass,
            softmaxScores,
            rankingRank
        FROM `etsy-data-warehouse-prod.search.sem_rel_hydrated_daily_requests` r
        JOIN `etsy-data-warehouse-prod.search.sem_rel_query_listing_metrics` USING (tableUUID)
        WHERE modelName = "v3-finetuned-llama-8b" 
        AND r.date between start_date and end_date
        AND pageNum = 1
        AND r.resultType = "organic"
    ),

    allrpc as (
        SELECT
            response.mmxRequestUUID as mmxRequestUUID,
            COALESCE((SELECT NULLIF(query, '') FROM UNNEST(request.filter.query.translations) WHERE language = 'en'), NULLIF(request.query, '')) query,
            DATE(queryTime) as date,
            IF(request.options.personalizationOptions.userId > 0, "SI", "SO") si_so
        FROM `etsy-searchinfra-gke-prod-2.thrift_mmx_listingsv2search_search.rpc_logs_*`
        WHERE request.OPTIONS.cacheBucketId LIKE "live%"
        AND request.options.csrOrganic
        AND request.options.searchPlacement IN ('wsg', 'allsr', 'wmg')
        AND request.query <> ''
        AND request.offset = 0
        AND DATE(queryTime) between start_date and end_date
        AND request.options.interleavingConfig IS NULL
        AND NOT EXISTS (
            SELECT * FROM UNNEST(request.context)
            WHERE key = "req_source" AND value = "bot"
        )
    ),

    us_so_page1 as (
        select sr.*
        from semrel_teacher_page1 sr
        left join allrpc using (mmxRequestUUID, query, date)
        where si_so = "SO"
        and userLanguage = "en-US"
        and userCountry = "US"
    ),

    agg_requests as (
        select 
            mmxRequestUUID, 
            guid,
            query, 
            queryBin,
            qisClass,
            date,
            platform, 
            count(*) as n_total, 
            sum(if(semrelClass = 'relevant', 1, 0)) as n_em,
            ARRAY_AGG(STRUCT(listingId, semrelClass, softmaxScores, rankingRank) ORDER BY rankingRank) AS listing_set
        from us_so_page1
        group by mmxRequestUUID, guid, date, query, platform, queryBin, qisClass
    ),

    valid_us_so_page1 as (
        select *, n_em / n_total as pct_em
        from agg_requests
        where (
            (platform = "web" and n_total = 48) or
            (platform = "mweb" and n_total = 34) or 
            (platform = "boe" and n_total = 28)
        )
    )

    select 
        mmxRequestUUID, 
        guid,
        query, 
        queryBin,
        qisClass,
        date,
        platform, 
        n_em,
        n_total,
        pct_em,
        ls.listingId,
        ls.semrelClass,
        ls.softmaxScores,
        ls.rankingRank    
    FROM valid_us_so_page1 r, UNNEST(r.listing_set) AS ls
    where pct_em <= 0.6
    order by date, mmxRequestUUID, query, rankingRank
)


-- ============================================================
-- 1. capture page 1 exact match locations
-- ============================================================
create or replace table `etsy-search-ml-dev.search.yzhang_emqueries_aug_base` as (
    with semrel_on_page as (
        select 
            mmxRequestUUID, guid, date, query, platform, queryBin, qisClass,
            sum(if(semrelClass = "relevant" and rankingRank >= 12 , 1, 0)) n_bottom_em,
            sum(if(semrelClass != "relevant" and rankingRank < 12 , 1, 0)) n_top_irr
        from `etsy-search-ml-dev.search.yzhang_emqueries_aug_base_raw`
        group by mmxRequestUUID, guid, date, query, platform, queryBin, qisClass
    )
    select raw_table.*
    from `etsy-search-ml-dev.search.yzhang_emqueries_aug_base_raw` raw_table
    join semrel_on_page
    using (mmxRequestUUID, guid, date, query, platform, queryBin, qisClass)
    where n_bottom_em >= 1
    and n_top_irr >= 2
)


-- feature hydration copied from https://gist.github.com/dblincoe/ab5f43efba23a6e292cf6b2e835d30f0

-- ============================================================
-- 2. Query hydration (only queries from base table)
-- ============================================================
create or replace table `etsy-search-ml-dev.search.yzhang_emqueries_aug_qfs` as (
    WITH ids AS (
        SELECT DISTINCT query
        FROM `etsy-search-ml-dev.search.yzhang_emqueries_aug_base`
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
    FROM `etsy-search-ml-dev.search.yzhang_emqueries_aug_base` b
    LEFT JOIN query_rewrites USING (query)
    LEFT JOIN query_entities USING (query)
    LEFT JOIN qtcv5 USING (query)
    LEFT JOIN qee USING (query)
)


-- ============================================================
-- 3. Listing hydration (only listingIds from base table)
-- ============================================================
CREATE OR REPLACE TABLE `etsy-search-ml-dev.search.yzhang_emqueries_aug_hydrated` AS (
  WITH ids AS (
    SELECT DISTINCT listingId
    FROM `etsy-search-ml-dev.search.yzhang_emqueries_aug_base`
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
  FROM `etsy-search-ml-dev.search.yzhang_emqueries_aug_qfs` hq
  LEFT JOIN lfb USING (listingId)
  LEFT JOIN listing_variations USING (listingId)
  LEFT JOIN listing_reviews USING (listingId)
  LEFT JOIN listing_images USING (listingId)
  LEFT JOIN listing_entities USING (listingId)
  LEFT JOIN listing_description USING (listingId)
)


-- ============================================================
-- 4. Analysis on the tables
-- ============================================================
------ August data
-- teacher sample: 525350 reqs, 433377 queries, 17311138 qlps  
-- raw:            48704 reqs,  47304 queries,  1857206  qlps  (~10%)
-- base:           34564 reqs,  33540 queries,  1315831  qlps  (~7%)

------ One day 2025-08-24
-- teacher sample: 21483 reqs, 20631 queries, 749210 qlps  
-- raw:            2133  reqs, 2102  queries, 80878 qlps  (~10%)
-- base:           1437  reqs, 1425  queries, 54871  qlps  (~7%)

declare start_date date default "2025-08-01";
declare end_date date default "2025-08-26";

with semrel_teacher_page1 as (
    SELECT
        mmxRequestUUID,
        guid,
        query,
        r.queryBin,
        r.qisClass,
        listingId,
        r.date,
        platform,
        userLanguage,
        userCountry,
        CASE 
            WHEN classId = 1 THEN 'not_relevant' 
            WHEN classId = 2 THEN 'partial'
            WHEN classId = 3 THEN 'relevant' 
        END AS semrelClass,
        softmaxScores,
        rankingRank
    FROM `etsy-data-warehouse-prod.search.sem_rel_hydrated_daily_requests` r
    JOIN `etsy-data-warehouse-prod.search.sem_rel_query_listing_metrics` USING (tableUUID)
    WHERE modelName = "v3-finetuned-llama-8b" 
    AND r.date between start_date and end_date
    AND pageNum = 1
    AND r.resultType = "organic"
),
tmp as (
    select distinct query, listingId
    from semrel_teacher_page1
)
select count(*) from tmp

with tmp as (
    select distinct query, listingId
    from `etsy-search-ml-dev.search.yzhang_emqueries_aug_base_raw`
)
select count(*) from tmp
