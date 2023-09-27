-- source: https://docs.google.com/document/d/1YMK3otWC0f4hxMEoiJ7GbB25DNweQyyE9cJ8oU29LO8/edit
-- etsy-sr-etl-prod.yfu_insights.query_taxo_boe
-- etsy-sr-etl-prod.yfu_insights.query_taxo_web

select top_purchased_taxo,
    IF(ARRAY_LENGTH(top_purchased_taxo) > 0, top_purchased_taxo[OFFSET(0)], NULL) AS query_top_purchased_taxo,
    IF(ARRAY_LENGTH(top_purchased_taxo) > 1, top_purchased_taxo[OFFSET(1)], NULL) AS query_2nd_purchased_taxo,
FROM `etsy-sr-etl-prod.yfu_insights.query_taxo_boe`
where top_purchased_taxo is not null
limit 200
-- top and 2nd purchased here is not referring to top / level 2 node of taxonomy paths, but the first and second item in category
-- e.g. ["clothing.women", "home.living_room.desk"] 
-- top is "clothing.women" and second is "home.living_room.desk"
-- we want top ["clothing", "home"], second ["clothing.women", "home.living_room"]

-- boe test
CREATE OR REPLACE TABLE `etsy-sr-etl-prod.yzhang.query_taxo_boe`
AS
(
    WITH query_taxo AS (
        SELECT 
            `key`,
            queryTaxoDemandFeatures_clickTopTaxonomyPaths as click_top_taxo,
            queryTaxoDemandFeatures_clickLevel2TaxonomyPaths as click_level2_taxo,
            queryTaxoDemandFeatures_purchaseTopTaxonomyPaths as purchase_top_taxo,
            queryTaxoDemandFeatures_purchaseLevel2TaxonomyPaths as purchase_level2_taxo,
            queryLevelMetrics_bin as query_bin
        FROM `etsy-ml-systems-prod.feature_bank_v2.query_feature_bank_most_recent`
        WHERE queryTaxoDemandFeatures_clickTopTaxonomyPaths is not NULL
    )
    SELECT
        ab_variant, visit_id, query, page_no, full_path,
        impressions, clicks, purchases, price_usd,
        click_top_taxo, click_level2_taxo, purchase_top_taxo, purchase_level2_taxo, query_bin
    FROM etsy-sr-etl-prod.search_ab_tests.taxo_demand_boe_0619 as ab_boe
    JOIN query_taxo ON ab_boe.query=query_taxo.key
)

-- web test
CREATE OR REPLACE TABLE `etsy-sr-etl-prod.yzhang.query_taxo_web`
AS
(
    WITH query_taxo AS (
        SELECT 
            `key`,
            queryTaxoDemandFeatures_clickTopTaxonomyPaths as click_top_taxo,
            queryTaxoDemandFeatures_clickLevel2TaxonomyPaths as click_level2_taxo,
            queryTaxoDemandFeatures_purchaseTopTaxonomyPaths as purchase_top_taxo,
            queryTaxoDemandFeatures_purchaseLevel2TaxonomyPaths as purchase_level2_taxo,
            queryLevelMetrics_bin as query_bin
        FROM `etsy-ml-systems-prod.feature_bank_v2.query_feature_bank_most_recent`
        WHERE queryTaxoDemandFeatures_clickTopTaxonomyPaths is not NULL
    )
    SELECT
        ab_variant, visit_id, query, page_no, full_path,
        impressions, clicks, purchases, price_usd,
        click_top_taxo, click_level2_taxo, purchase_top_taxo, purchase_level2_taxo, query_bin
    FROM etsy-sr-etl-prod.search_ab_tests.taxo_demand_web_0612 as ab_web
    JOIN query_taxo ON ab_web.query=query_taxo.key
)

-- boe with query intent
CREATE OR REPLACE TABLE etsy-sr-etl-prod.yzhang.query_taxo_boe_full AS (
    WITH query_intent_data as (
        select query_raw, inference.label as query_intent
        from `etsy-data-warehouse-prod.arizona.query_intent_labels`
        QUALIFY ROW_NUMBER() OVER(PARTITION BY query_raw ORDER BY inference.confidence DESC) = 1
    )
    select exp_data.*, query_intent_data.query_intent
    from `etsy-sr-etl-prod.yzhang.query_taxo_boe` exp_data
    left join query_intent_data
    on exp_data.query = query_intent_data.query_raw
)

-- web with query intent
CREATE OR REPLACE TABLE etsy-sr-etl-prod.yzhang.query_taxo_web_full AS (
    WITH query_intent_data as (
        select query_raw, inference.label as query_intent
        from `etsy-data-warehouse-prod.arizona.query_intent_labels`
        QUALIFY ROW_NUMBER() OVER(PARTITION BY query_raw ORDER BY inference.confidence DESC) = 1
    )
    select exp_data.*, query_intent_data.query_intent
    from `etsy-sr-etl-prod.yzhang.query_taxo_web` exp_data
    left join query_intent_data
    on exp_data.query = query_intent_data.query_raw
)


-- price
SELECT 
    CASE
       WHEN page_no > 3 THEN 4
       ELSE page_no
    END AS page_no_reduced, 
    ab_variant, 
    sum(impressions) as total_impression,
    sum(clicks) as total_clicks, 
    sum(purchases) as total_purchases,
    avg(price_usd) as avg_price
FROM etsy-sr-etl-prod.yzhang.query_taxo_web_full
GROUP BY page_no_reduced, ab_variant
ORDER BY page_no_reduced, ab_variant DESC


-- query_bin
SELECT 
    CASE
       WHEN page_no > 3 THEN 4
       ELSE page_no
    END AS page_no_reduced, 
    ab_variant, 
    query_bin,
    sum(clicks) / sum(impressions) as ctr, 
    sum(purchases) / sum(clicks) as post_click_cvr,
FROM etsy-sr-etl-prod.yzhang.query_taxo_web_full
GROUP BY page_no_reduced, query_bin, ab_variant
ORDER BY page_no_reduced, query_bin, ab_variant DESC


-- query intent
SELECT 
    CASE
       WHEN page_no > 3 THEN 4
       ELSE page_no
    END AS page_no_reduced, 
    ab_variant, 
    query_intent,
    sum(clicks) / (sum(impressions) + 0.000001) as ctr, 
    sum(purchases) / (sum(clicks) + 0.000001) as post_click_cvr,
FROM etsy-sr-etl-prod.yzhang.query_taxo_web_full
where query_intent is not null and query_intent != ''
GROUP BY page_no_reduced, query_intent, ab_variant
ORDER BY page_no_reduced, query_intent, ab_variant DESC