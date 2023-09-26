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

CREATE OR REPLACE TABLE etsy-sr-etl-prod.yzhang.query_taxo_web_corrected AS (
    WITH exp_data AS (
        SELECT 
            ab_variant, visit_id, query, page_no, full_path,
            impressions, clicks, purchases, price_usd,
            top_clicked_taxo as query_click_level2_taxo, top_purchased_taxo as query_purchase_level2_taxo
        FROM `etsy-sr-etl-prod.yfu_insights.query_taxo_web`
    ),
    join_intent as (
        select exp_data.*, tmp.query_intent
        from exp_data
        left join (
            select query_raw, inference.label as query_intent
            from `etsy-data-warehouse-prod.arizona.query_intent_labels`
            QUALIFY ROW_NUMBER() OVER(PARTITION BY query_raw ORDER BY inference.confidence DESC) = 1
        ) as tmp
        on exp_data.query = tmp.query_raw
    ),
    full_exp_data as (
        select join_intent.*, query_fl.queryLevelMetrics_bin as query_bin
        from join_intent
        left join `etsy-ml-systems-prod.feature_bank_v2.query_feature_bank_most_recent` query_fl
        on join_intent.query = query_fl.key
    )
    select * from full_exp_data
)

