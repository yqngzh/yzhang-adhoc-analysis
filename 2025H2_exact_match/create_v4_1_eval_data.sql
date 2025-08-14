-- create base data
create or replace table `etsy-search-ml-dev.yzhang.exact_matches_base_data_v4_1` as (
    with excel_data as (
        SELECT
            mmxRequestUUID, 
            query, 
            listing_id, 
            Relevance_Class AS relevance_class,
            Product_type_in_query AS product_type_in_query,
            Product_type_in_listing_if_mismatch AS product_type_in_listing_if_mismatch,
            Subsitute___Complementary AS subsitute_complementary,
            Descriptor_Mismatch AS descriptor_mismatch
        FROM `etsy-data-warehouse-dev.dblincoe.exact_matches_annotation_july_2025`
    ),
    source_from_semrel_table as (
        SELECT DISTINCT
            r.etsyUUID,
            r.platform,
            r.userCountry,
            r.userLanguage,
            r.mmxRequestUUID,
            r.query AS query,
            REGEXP_CONTAINS(query, '(\?i)\\bgift|\\bfor (\\bhim|\\bher|\\bmom|\\bdad|\\bmother|\\bfather|\\bdaughter|\\bson|\\bwife|\\bhusband|\\bpartner|\\baunt|\\buncle|\\bniece|\\bnephew|\\bfiance|\\bcousin|\\bin law|\\bboyfriend|\\bgirlfriend|\\bgrand|\\bfriend|\\bbest friend)') queryIsGift,
            r.listingId,
            "partial" as v3_teacher_label
        FROM  `etsy-data-warehouse-prod.search.sem_rel_hydrated_daily_requests` r
        JOIN `etsy-data-warehouse-prod.search.sem_rel_query_listing_metrics` m USING (tableUUID, date)
        WHERE r.date BETWEEN '2025-06-01' AND '2025-06-30'
        AND m.modelName = 'v3-finetuned-llama-8b'
        AND m.classId = 2
        AND r.pageNum IN (1, 2, 3)
        AND r.userLanguage = 'en-US'
    ),
    merged as (
        select distinct
            etsyUUID, platform, userCountry, userLanguage,
            excel_data.query, 
            "" as queryEn,
            listingid as listingId,
            relevance_class as gt_label,
            product_type_in_query, 
            product_type_in_listing_if_mismatch,
            subsitute_complementary,
            descriptor_mismatch,
            queryIsGift,
            v3_teacher_label,
            "partial_purchases" as anno_data_source,
        from excel_data
        left join source_from_semrel_table on (
            excel_data.query = source_from_semrel_table.query and
            excel_data.listing_id = source_from_semrel_table.listingId
        )
    ),
    ql_count as (
        select query, listingId, count(*) as cnt 
        from merged
        group by query, listingId
    ),
    deduplicated as (
        select merged.*
        from merged
        join ql_count using (query, listingId)
        where cnt = 1
    )
    select
        GENERATE_UUID() AS row_id,
        *
    from deduplicated
)
-- remaining 422 pairs; could not join on mmxRequestUUID because only 3 pairs left


-- hydrate features using same methods as V3
---- query features first
create or replace table `etsy-search-ml-dev.yzhang.exact_matches_hydrated_data_v4_1_query` as (
    with query_rewrites AS (
        SELECT key AS query, STRING_AGG(unnested_value, ", ") AS queryRewrites
        FROM `etsy-search-ml-dev.mission_understanding.smu_query_rewriting_v2_dpo_semrel`, 
            UNNEST(value) AS unnested_value
        GROUP BY key
    ),
    qe_raw AS (
        SELECT DISTINCT
            searchQuery AS query,
            entities AS queryEntities
        FROM `etsy-data-warehouse-prod.arizona.query_entity_features`
    ),
    query_entities AS (
        SELECT * 
        FROM qe_raw
        QUALIFY ROW_NUMBER() OVER(PARTITION BY query ORDER BY rand()) = 1
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
    qtcv5 as (
      select distinct
       coalesce(s.query, b.query) as query,
       coalesce(s.full_path, b.full_path) as queryTaxoFullPath,
      from `etsy-data-warehouse-prod.mission_understanding.query_taxonomy_seller` s
      full outer join `etsy-data-warehouse-prod.mission_understanding.query_taxonomy_buyer` b
       using(query)
    ),
    qee_raw AS (
      select distinct
        searchQuery as query,
        fandom,
        motif,
        style,
        material,
        color,
        technique,
        tangibleItem,
        size,
        occasion,
        customization,
        age,
        price,
        quantity,
        recipient
      from `etsy-search-ml-prod.mission_understanding.query_entity_features`
    ),
    qee AS (
      select *
      from qee_raw
      QUALIFY ROW_NUMBER() OVER (PARTITION BY query ORDER BY RAND()) = 1 
    )
    select 
        b.*,
        queryRewrites,
        queryEntities,
        queryBin as seg_queryBin,
        qisClass as seg_qisClass,
        queryTaxoFullPath as seg_queryTaxoFullPath,
        SPLIT(queryTaxoFullPath, ".")[OFFSET(0)] as seg_queryTaxoTop,
        fandom,
        motif,
        style,
        material,
        color,
        technique,
        tangibleItem,
        size,
        occasion,
        customization,
        age,
        price,
        quantity,
        recipient
    from `etsy-search-ml-dev.yzhang.exact_matches_base_data_v4_1` b
    left join query_rewrites using (query)
    left join query_entities using (query)
    left join qlm using (query)
    left join qisv3 using (query)
    left join qtcv5 using (query)
    left join qee using (query)
)

--- then, listing features
create or replace table `etsy-search-ml-dev.yzhang.exact_matches_hydrated_data_v4_1` as (
    with lfb AS (
        SELECT 
            key as listingId,
            COALESCE(verticaListings_title, verticaListingTranslations_primaryLanguageTitle, "") AS listingTitle,
            IFNULL(verticaListings_taxonomyPath, "") AS listingTaxo,
            IFNULL(verticaListings_tags, "") listingTags,
            (SELECT STRING_AGG(element, ';') FROM UNNEST(kbAttributesV2_sellerAttributesV2.list)) listingAttributes,
            IFNULL(verticaSellerBasics_shopName, "") listingShopName,
            IFNULL(verticaListings_description, "")  listingDescription,
            (SELECT STRING_AGG(element, ', ') FROM UNNEST(descNgrams_ngrams.list)) listingDescNgrams,
            IFNULL(listingLlmFeatures_llmHeroImageDescription, "") listingHeroImageCaption,
            IFNULL(verticaShopSettings_primaryLanguage, "") as shop_primaryLanguage,
            IFNULL(localeFeatures_listingCountry, "") as listingCountry
        FROM `etsy-ml-systems-prod.feature_bank_v2.listing_feature_bank_most_recent`
    ),
    grouped_variation_values AS (
        SELECT 
            listing_id, 
            attribute_name,
            STRING_AGG(attribute_value, ', ') AS grouped_attributes
        FROM `etsy-data-warehouse-prod.listing_mart.listing_variation_attributes`
        GROUP BY listing_id, attribute_name
    ),
    listing_variations AS (
        SELECT 
            listing_id as listingId,
            STRING_AGG(CONCAT(attribute_name, ': ', grouped_attributes), '; ') AS listingVariations
        FROM grouped_variation_values
        GROUP BY listing_id
    ),
    review_raw AS (
        SELECT listing_id, review, DATE(TIMESTAMP_SECONDS(update_date)) review_last_update_date, 
        FROM `etsy-data-warehouse-prod.etsy_shard.listing_review`
        WHERE review IS NOT null AND review != ""
    ),
    recent_five_reviews AS (
        SELECT *
        FROM review_raw
        QUALIFY ROW_NUMBER() OVER(PARTITION BY listing_id ORDER BY review_last_update_date DESC) <= 5
    ),
    listing_reviews AS (
        SELECT
            listing_id AS listingId, 
            STRING_AGG(review, ' | ') listingReviews
        FROM recent_five_reviews
        GROUP BY listing_id
    ),
    listing_images AS (
        SELECT 
            listing_id listingId,
            STRING_AGG(url, ';' ORDER BY img_rank ASC) listingImageUrls
        FROM `etsy-data-warehouse-prod.computer_vision.listing_image_paths`
        GROUP BY listing_id
    )
    select 
        hq.*,
        listingCountry,
        shop_primaryLanguage,
        listingTitle,
        "" as listingTitleEn,
        listingTaxo,
        listingTags,
        listingAttributes,
        listingShopName,
        listingDescription,
        "" as listingDescriptionEn,
        listingDescNgrams,
        listingImageUrls,
        listingHeroImageCaption,
        listingVariations,
        listingReviews
    from `etsy-search-ml-dev.yzhang.exact_matches_hydrated_data_v4_1_query` hq
    left join lfb using (listingId)
    left join listing_variations using (listingId)
    left join listing_reviews using (listingId)
    left join listing_images using (listingId)
)
