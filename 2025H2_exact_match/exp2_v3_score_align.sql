-- get requests with at least 1 tight purchase from day's attributed instance data
create or replace table `etsy-search-ml-dev.search.yzhang_em_exp2_purchase_tight_2025_08_19` as (
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
            "web" as platform,
            candidateInfo.retrievalInfo.semrelRelevantScore,
            candidateInfo.retrievalInfo.semrelPartialScore,
            candidateInfo.retrievalInfo.semrelIrrelevantScore,
            attributions
        from `etsy-ml-systems-prod.attributed_instance.query_pipeline_web_organic_tight_2025_08_19`, unnest(contextualInfo) as ctx
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
            "boe" as platform,
            candidateInfo.retrievalInfo.semrelRelevantScore,
            candidateInfo.retrievalInfo.semrelPartialScore,
            candidateInfo.retrievalInfo.semrelIrrelevantScore,
            attributions
        from `etsy-ml-systems-prod.attributed_instance.query_pipeline_boe_organic_tight_2025_08_19`, unnest(contextualInfo) as ctx
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
            "market" as platform,
            candidateInfo.retrievalInfo.semrelRelevantScore,
            candidateInfo.retrievalInfo.semrelPartialScore,
            candidateInfo.retrievalInfo.semrelIrrelevantScore,
            attributions
        from `etsy-ml-systems-prod.attributed_instance.query_pipeline_market_web_organic_2025_08_19`, unnest(contextualInfo) as ctx
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
            platform,
            semrelRelevantScore as v2RelevantScore,
            semrelPartialScore as v2PartialScore,
            semrelIrrelevantScore as v2IrrelevantScore,
            attributions
        from qlp_raw
    ),
    lfb as (
        select 
            key as listingId,
            IFNULL(
                COALESCE(NULLIF(verticaListingTranslations_machineTranslatedEnglishTitle, ''), NULLIF(verticaListings_title, '')),
                ""
            ) listingTitle,
            IFNULL(verticaSellerBasics_shopName, "") listingShopName,
            IFNULL(listingLlmFeatures_llmHeroImageDescription, "") listingHeroImageCaption,
            IFNULL((SELECT STRING_AGG(element, ', ') FROM UNNEST(descNgrams_ngrams.list)), "") listingDescNgrams,
            IFNULL(verticaListings_taxonomyPath, "") listingTaxo,
            IFNULL(verticaListings_tags, "") listingTags 
        from `etsy-ml-systems-prod.feature_bank_v2.listing_feature_bank_2025-08-19`
    ),
    full_result as (
        select distinct *
        from qlp
        left join lfb using (listingId)
        where query is not null and query != ""
    ),
    purchased_requests as (
        select distinct requestUUID
        from qlp_raw
        where "purchase" in unnest(attributions)
    )
    select * 
    from full_result
    where requestUUID in (
        select * from purchased_requests
    )
)
