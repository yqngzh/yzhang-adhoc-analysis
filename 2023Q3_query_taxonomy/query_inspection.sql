-- taking 20 most query level GMS queries from each bin
SELECT `key` as query,
FROM `etsy-ml-systems-prod.feature_bank_v2.query_feature_bank_2023-10-31`,
  `etsy-sr-etl-prod.yzhang.total_query_level_gms_1031`
where queryLevelMetrics_bin = "head"
order by queryLevelMetrics_gms desc
limit 20


select `key` as query,
  queryTaxoDemandFeatures_impressionTopTaxonomyPaths.list as impression_paths,
  queryTaxoDemandFeatures_impressionTopTaxonomyCounts.list as impression_counts,
  queryTaxoDemandFeatures_purchaseTopTaxonomyPaths.list as purchase_paths,
  queryTaxoDemandFeatures_purchaseTopTaxonomyCounts as purchase_counts,
FROM `etsy-ml-systems-prod.feature_bank_v2.query_feature_bank_2023-10-31`
where `key` = 'gift'


select `key` as query,
  queryTaxoDemandFeatures_impressionLevel2TaxonomyPaths as impression_paths,
  queryTaxoDemandFeatures_impressionLevel2TaxonomyCounts as impression_counts,
  queryTaxoDemandFeatures_purchaseLevel2TaxonomyPaths as purchase_paths,
  queryTaxoDemandFeatures_purchaseLevel2TaxonomyCounts as purchase_counts,
FROM `etsy-ml-systems-prod.feature_bank_v2.query_feature_bank_2023-10-31`
where `key` = 'gift'