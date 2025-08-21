----  Attributed instances
select
    requestUUID, visitId, position,
    attributions,
    ctx.docInfo.queryInfo.query, 
    ctx.docInfo.queryInfo.queryLevelMetrics.*,
    ctx.docInfo.queryInfo.queryTaxoDemandFeatures.*,
from `etsy-ml-systems-prod.attributed_instance.query_pipeline_web_organic_tight_2025_08_19`, 
    unnest(contextualInfo) as ctx
where ctx.docInfo.queryInfo.query in ('personalized gift', 'personalise gift', 'mother day', 'mothers day', 'gift for him', 'gifts for him', 'gift')
order by requestUUID, position



----  Feature bank
SELECT key, queryLevelMetrics_bin, queryLevelMetrics_gms 
FROM `etsy-ml-systems-prod.feature_bank_v2.query_feature_bank_most_recent`

SELECT 
    key as listingId,
    -- activeListingBasics_priceUsd as price
    (select value from unnest(listingWeb_price.key_value) where key = 'US') price
FROM `etsy-ml-systems-prod.feature_bank_v2.listing_feature_bank_most_recent`