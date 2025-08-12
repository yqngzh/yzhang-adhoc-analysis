-- Build input table with required columns
-- query, listingId, listingTitle, listingShopName, listingHeroImageCaption listingDescNgrams
create or replace table `etsy-search-ml-dev.search.yzhang_tire_solr_entity` as (
    with lfb as (
        select 
            key as listingId,
            IFNULL(
                COALESCE(NULLIF(verticaListings_title, ""), verticaListingTranslations_primaryLanguageTitle),
                ""
            ) listingTitle,
            IFNULL(verticaSellerBasics_shopName, "") listingShopName,
            IFNULL(listingLlmFeatures_llmHeroImageDescription, "") listingHeroImageCaption,
            (SELECT STRING_AGG(element, ', ') FROM UNNEST(descNgrams_ngrams.list)) listingDescNgrams,
        from `etsy-ml-systems-prod.feature_bank_v2.listing_feature_bank_most_recent`
    ),
    qlp as (
        select
            request.query,
            listingId, 
        from `etsy-searchinfra-gke-dev.thrift_tire_listingsv2search_search.rpc_logs_a140360b`,
            UNNEST(response.listingIds) AS listingId WITH OFFSET position
        where request.query != ""
        and tireRequestContext.tireTestv2Id = "FOCyqZESheEmPzQB1oFS"
    )
    select *
    from qlp
    left join lfb using (listingId)
)


-- After inference, join with predictions with original table
with tire_results as (
    select distinct
        response.mmxRequestUUID as uuid,
        request.query,
        listingId, 
        tireRequestContext.variant as variant,
    from `etsy-searchinfra-gke-dev.thrift_tire_listingsv2search_search.rpc_logs_a140360b`,
        UNNEST(response.listingIds) AS listingId WITH OFFSET position
    where request.query != ""
    and tireRequestContext.tireTestv2Id = "FOCyqZESheEmPzQB1oFS"
),
tire_with_semrel as (
    select 
        tire_results.*,
        semrelLabel,
        if(semrelLabel = 'relevant', 1, 0) is_relevant
    from tire_results
    left join `etsy-search-ml-dev.search.semrel_adhoc_yzhang_tire_solr_entity`
    using (query, listingId)
)
select 
    variant, 
    count(*) as n_pairs_in_variants, 
    sum(is_relevant) as n_exact_matches,
    sum(is_relevant) / count(*) as pct_exact_matches
from tire_with_semrel
group by variant