create or replace table `etsy-sr-etl-prod.yzhang.qtd_boosting_results` as (
    WITH query_taxo AS (
        SELECT 
            `key` as query,
            queryTaxoDemandFeatures_purchaseTopTaxonomyPaths as purchase_top_taxo,
            queryTaxoDemandFeatures_purchaseLevel2TaxonomyPaths as purchase_level2_taxo,
            queryLevelMetrics_bin as query_bin
        FROM `etsy-ml-systems-prod.feature_bank_v2.query_feature_bank_most_recent`
        WHERE queryTaxoDemandFeatures_purchaseLevel2TaxonomyPaths is not NULL
    ),
    listing_taxo as (
        select 
            listing_id,
            split(lv.full_path, '.')[safe_offset(0)] as top_category, 
            split(lv.full_path, '.')[safe_offset(1)] as second_category, 
        from `etsy-data-warehouse-prod.listing_mart.listing_vw` lv
    ),
    listing_taxo_clean as (
        select 
            listing_id,
            top_category as listing_top_category, 
            if (top_category is not null and second_category is not null, concat(top_category, '.', second_category), null) as listing_level2_category,
        from listing_taxo
    )
    select 
        qtd.*,
        purchase_top_taxo,
        purchase_level2_taxo,
        listing_top_category,
        listing_level2_category
    from `etsy-sr-etl-prod.kbekal_insights.qtd_boosting_top` qtd
    left join query_taxo 
    on query_taxo.query = qtd.query
    left join listing_taxo_clean
    on qtd.listing_id = listing_taxo_clean.listing_id
)

create or replace table `etsy-sr-etl-prod.yzhang.qtd_boosting_merge` as (
    WITH query_taxo AS (
        SELECT 
            `key` as query,
            queryTaxoDemandFeatures_purchaseTopTaxonomyPaths as purchase_top_taxo,
            queryTaxoDemandFeatures_purchaseLevel2TaxonomyPaths as purchase_level2_taxo,
            queryLevelMetrics_bin as query_bin
        FROM `etsy-ml-systems-prod.feature_bank_v2.query_feature_bank_most_recent`
        WHERE queryTaxoDemandFeatures_purchaseLevel2TaxonomyPaths is not NULL
    )
    select 
        overlap.*,
        query_taxo.purchase_top_taxo,
        query_taxo.purchase_level2_taxo,
    from `etsy-sr-etl-prod.yzhang.qtd_boosting_results_overlap` overlap
    left join query_taxo
    on overlap.query = query_taxo.query
)

select behavior, avg(top_overlap) as top_match
from `etsy-sr-etl-prod.yzhang.qtd_boosting_merge`
group by behavior

select behavior, avg(level2_overlap) as level2_match
from `etsy-sr-etl-prod.yzhang.qtd_boosting_merge`
group by behavior
