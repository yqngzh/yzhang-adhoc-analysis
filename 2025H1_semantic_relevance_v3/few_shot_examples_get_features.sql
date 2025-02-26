CREATE OR REPLACE TABLE `etsy-sr-etl-prod.yzhang.semrel_few_shot_examples_v0_full` AS (
    WITH ql_pairs AS (
        SELECT * 
        FROM `etsy-sr-etl-prod.yzhang.semrel_few_shot_examples_v0`
    ),
    listing_fb AS (
    SELECT 
        key as listingId,
        IFNULL(
            COALESCE(NULLIF(verticaListings_title, ""), verticaListingTranslations_primaryLanguageTitle),
            ""
        ) listingTitle,
        IFNULL(verticaListings_description, "") listingDescription,
        IFNULL(verticaListings_tags, "") listingTags,
        IFNULL(verticaListings_taxonomyPath, "") listingTaxo,
        IFNULL(verticaSellerBasics_shopName, "") listingShopName,
        (SELECT STRING_AGG(element, ';') FROM UNNEST(kbAttributesV2_sellerAttributesV2.list)) AS listingAttributes,
        (SELECT STRING_AGG(element, ', ') FROM UNNEST(descNgrams_ngrams.list)) listingDescNgrams,
        IFNULL(listingLlmFeatures_llmHeroImageDescription, "") listingHeroImageCaption,
    FROM `etsy-ml-systems-prod.feature_bank_v2.listing_feature_bank_2025-02-25`
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
    ),
    query_rewrites AS (
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
    )
    SELECT 
    query, 
    listingId, 
    queryRewrites,
    queryEntities,
    listingTitle,
    listingTaxo,
    listingTags,
    listingAttributes,
    listingShopName,
    listingDescription,
    listingDescNgrams,
    listingImageUrls,
    listingHeroImageCaption,
    listingVariations,
    listingReviews,
    FROM ql_pairs
    LEFT JOIN listing_fb USING (listingId)
    LEFT JOIN listing_variations USING (listingId)
    LEFT JOIN listing_reviews USING (listingId)
    LEFT JOIN listing_images USING (listingId)
    LEFT JOIN query_rewrites USING (query)
    LEFT JOIN query_entities USING (query)
)
