with tmp_data as (
  SELECT ql.guid, ql.query, d.listingTitle, ql.relevanceScore, d.rankingRank, r.relevanceNDCG
  FROM `etsy-data-warehouse-prod.search.sem_rel_query_listing_metrics` ql
  JOIN `etsy-data-warehouse-prod.search.sem_rel_requests_metrics` r
    ON ql.date = r.date
    AND ql.modelName = r.modelName
    AND ql.guid = r.guid
  JOIN `etsy-data-warehouse-prod.search.sem_rel_hydrated_daily_requests` d
    ON ql.date = d.date
    AND ql.guid = d.guid
    AND ql.listingId = d.listingId
  WHERE ql.date = "2024-04-05"
    AND ql.modelName = "bert-cern-l24-h1024-a16"
    AND d.rankingRank IS NOT NULL
    AND ql.pageNum = 1
  ORDER BY ql.guid, d.rankingRank
)
select avg(relevanceNDCG) from tmp_data

with tmp_data as (
  SELECT ql.guid, ql.query, d.listingTitle, ql.relevanceScore, d.rankingRank, r.relevanceNDCG
  FROM `etsy-data-warehouse-prod.search.sem_rel_query_listing_metrics` ql
  JOIN `etsy-data-warehouse-prod.search.sem_rel_requests_metrics` r
    ON ql.date = r.date
    AND ql.modelName = r.modelName
    AND ql.guid = r.guid
  JOIN `etsy-data-warehouse-prod.search.sem_rel_hydrated_daily_requests` d
    ON ql.date = d.date
    AND ql.guid = d.guid
    AND ql.listingId = d.listingId
  WHERE ql.date = "2024-04-05"
    AND ql.modelName = "bert-cern-l24-h1024-a16"
    AND d.rankingRank IS NOT NULL
    AND ql.pageNum = 1
    AND d.guid = "000a93dd-e7d5-4989-9f24-0ba78185b714"
    AND d.rankingRank <= 9
  ORDER BY d.rankingRank
)
select * from tmp_data
