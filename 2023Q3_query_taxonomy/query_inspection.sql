-- taking 20 most query level GMS queries from each bin
SELECT `key` as query,
FROM `etsy-ml-systems-prod.feature_bank_v2.query_feature_bank_2023-10-31`,
  `etsy-sr-etl-prod.yzhang.total_query_level_gms_1031`
where queryLevelMetrics_bin = "head"
order by queryLevelMetrics_gms desc
limit 20

select `key` as query,
  queryTaxoDemandFeatures_purchaseLevel2TaxonomyPaths,
  queryTaxoDemandFeatures_purchaseLevel2TaxonomyCounts,
  queryTaxoDemandFeatures_purchaseLevel2TaxonomyPrices,
FROM `etsy-ml-systems-prod.feature_bank_v2.query_feature_bank_2023-10-31`
where `key` = 'gift'