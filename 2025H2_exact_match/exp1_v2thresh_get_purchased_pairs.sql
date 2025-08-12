-- get purchased pairs data
create or replace table `etsy-search-ml-dev.search.yzhang_emcf_purchase_tight_2025_07_20` as (
    with qlp_raw as (
        -- web
        select 
            requestUUID,
            visitId,
            clientProvidedInfo.query.queryEn client_query_en,
            clientProvidedInfo.query.query client_query,
            ctx.docInfo.queryInfo.query context_query,
            candidateInfo.docInfo.listingInfo.listingId,
            position,
            ctx.userInfo.userId,
            clientProvidedInfo.user.userCountry,
            clientProvidedInfo.user.userPreferredLanguage,
            candidateInfo.docInfo.listingInfo.localeFeatures.listingCountry,
            candidateInfo.docInfo.shopInfo.verticaShopSettings.primaryLanguage,
            "web" as platform
        from `etsy-ml-systems-prod.attributed_instance.query_pipeline_web_organic_tight_2025_07_20`, unnest(contextualInfo) as ctx
        where "purchase" in unnest(attributions)
        -- boe
        union all
        select  
            requestUUID,
            visitId,
            clientProvidedInfo.query.queryEn client_query_en,
            clientProvidedInfo.query.query client_query,
            ctx.docInfo.queryInfo.query context_query,
            candidateInfo.docInfo.listingInfo.listingId,
            position,
            ctx.userInfo.userId,
            clientProvidedInfo.user.userCountry,
            clientProvidedInfo.user.userPreferredLanguage,
            candidateInfo.docInfo.listingInfo.localeFeatures.listingCountry,
            candidateInfo.docInfo.shopInfo.verticaShopSettings.primaryLanguage,
            "boe" as platform
        from `etsy-ml-systems-prod.attributed_instance.query_pipeline_boe_organic_tight_2025_07_20`, unnest(contextualInfo) as ctx
        where "purchase" in unnest(attributions)
        -- market
        union all
        select
            requestUUID,
            visitId,
            clientProvidedInfo.query.queryEn client_query_en,
            clientProvidedInfo.query.query client_query,
            ctx.docInfo.queryInfo.query context_query,
            candidateInfo.docInfo.listingInfo.listingId,
            position,
            ctx.userInfo.userId,
            clientProvidedInfo.user.userCountry,
            clientProvidedInfo.user.userPreferredLanguage,
            candidateInfo.docInfo.listingInfo.localeFeatures.listingCountry,
            candidateInfo.docInfo.shopInfo.verticaShopSettings.primaryLanguage,
            "market" as platform
        from `etsy-ml-systems-prod.attributed_instance.query_pipeline_market_web_organic_2025_07_20`, unnest(contextualInfo) as ctx
        where "purchase" in unnest(attributions)
    ),
    qlp as (
        select 
            requestUUID,
            visitId,
            case
                when client_query_en is not null and client_query_en != "" then client_query_en
                when client_query is not null and client_query != "" then client_query
                else context_query
            end as query,
            listingId,
            position,
            userId,
            userCountry,
            userPreferredLanguage as userLanguage,
            listingCountry,
            primaryLanguage as listingLanguage,
            platform
        from qlp_raw
    ),
    lfb as (
        select 
            key as listingId,
            IFNULL(
                COALESCE(NULLIF(verticaListings_title, ''), NULLIF(verticaListingTranslations_machineTranslatedEnglishTitle, '')),
                ""
            ) listingTitle,
            IFNULL(verticaSellerBasics_shopName, "") listingShopName,
            IFNULL(listingLlmFeatures_llmHeroImageDescription, "") listingHeroImageCaption,
            IFNULL((SELECT STRING_AGG(element, ', ') FROM UNNEST(descNgrams_ngrams.list)), "") listingDescNgrams,
            IFNULL(verticaListings_taxonomyPath, "") listingTaxo,
            IFNULL(verticaListings_tags, "") listingTags 
        from `etsy-ml-systems-prod.feature_bank_v2.listing_feature_bank_2025-07-20`
    )
    select distinct *
    from qlp
    left join lfb using (listingId)
    where query is not null and query != ""
)
-- IFNULL(
--     COALESCE(
--         NULLIF(candidateInfo.docInfo.listingInfo.verticaListings.title, ''), 
--         NULLIF(candidateInfo.docInfo.listingInfo.verticaListingTranslations.machineTranslatedEnglishTitle, '')
--     ), ""
-- ) listingTitle,
-- IFNULL(candidateInfo.docInfo.shopInfo.verticaSellerBasics.shopName, "") listingShopName,
-- IFNULL(candidateInfo.docInfo.listingInfo.listingLlmFeatures.llmHeroImageDescription, "") listingHeroImageCaption,
-- ARRAY_TO_STRING(IFNULL(candidateInfo.docInfo.listingInfo.descNgrams.ngrams, [""]), ', ') listingDescNgrams,

with tmp as (
    select distinct 
        query, listingId, 
        listingTitle, listingShopName, listingHeroImageCaption, listingDescNgrams, 
        listingTaxo, listingTags
    from `etsy-search-ml-dev.search.yzhang_emcf_purchase_tight_2025_07_20`
)
select count(*) from tmp
-- 2025-07-20: 165678 distinct query listing pairs, from 179978 requests
-- 2025-07-21: 172960 distinct query listing pairs, from 186728 requests

-- run through V3 teacher to get `semrel_adhoc_yzhang_emcf_purchase_tight_2025_07_20`

create or replace table `etsy-search-ml-dev.search.yzhang_emcf_purchase_results_tight_2025_07_20` as (
    with qlm AS (
      select distinct query_raw as query, bin as queryBin 
      from `etsy-data-warehouse-prod.rollups.query_level_metrics_raw`
    ),
    qee as (
        select distinct input.query, "has_entity" as entities
        from `etsy-search-ml-prod.mission_understanding.query_entity_extraction_v2_canonical_values`
    )
    select 
        ori.*,
        queryBin,
        entities,
        semrelLabel
    from `etsy-search-ml-dev.search.yzhang_emcf_purchase_tight_2025_07_20` ori
    left join `etsy-search-ml-dev.search.semrel_adhoc_yzhang_emcf_purchase_tight_2025_07_20`
    using (query, listingId)
    left join qlm using (query)
    left join qee using(query)
)

       