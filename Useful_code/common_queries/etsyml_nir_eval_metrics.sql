with request_group as (
  SELECT 
    requestUUID,
    query,
    AVG(metrics.purchase.recall10) as recall10,
    AVG(metrics.purchase.recall100) as recall100,
    AVG(metrics.purchase.recall1000) as recall1000
  FROM `etsy-search-ml-dev.search_retrieval.neural_ir_eval` 
  WHERE
    modelName = "neural_ir_test"
  GROUP BY 1,2
),
query_group as (
  SELECT 
    query,
    AVG(recall10) as recall10,
    AVG(recall100) as recall100,
    AVG(recall1000) as recall1000
  FROM request_group
  GROUP BY 1
)
SELECT
  AVG(recall10) as recall10,
  AVG(recall100) as recall100,
  AVG(recall1000) as recall1000
FROM query_group;