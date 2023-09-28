with tmp as (
  SELECT `key` as query,
    queryTaxoDemandFeatures_clickTopTaxonomyPaths as ctop,
    queryTaxoDemandFeatures_clickLevel2TaxonomyPaths as csec,
    queryTaxoDemandFeatures_purchaseTopTaxonomyPaths as ptop,
    queryTaxoDemandFeatures_purchaseLevel2TaxonomyPaths as psec,
  FROM `etsy-ml-systems-prod.feature_bank_v2.query_feature_bank_most_recent` 
)
select * from tmp
where ptop is null and psec is not null
LIMIT 1000


with tmp as (
  SELECT `key` as query,
    queryTaxoDemandFeatures_clickTopTaxonomyPaths as ctop,
    queryTaxoDemandFeatures_clickLevel2TaxonomyPaths as csec,
    queryTaxoDemandFeatures_purchaseTopTaxonomyPaths as ptop,
    queryTaxoDemandFeatures_purchaseLevel2TaxonomyPaths as psec,
  FROM `etsy-ml-systems-prod.feature_bank_v2.query_feature_bank_most_recent` 
)
select * from tmp
where array_length(ctop.list) = 0 and array_length(csec.list) > 0
LIMIT 1000


-- top level Null <=> second level Null (both click and purchase)
-- no array length = 0
-- if only top node level, second level empty string ''