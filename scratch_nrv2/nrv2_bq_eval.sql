SELECT 
	AVG(metrics.purchase.ndcg_10), 
FROM 
	`etsy-search-ml-prod.search.second_pass_eval`
WHERE modelName="nrv2-semrel-uni-serve-tm-so"
AND eval_date="2025-05-25"
AND source="web_purchase"
AND userCountry="US"
AND tags.userId > 0
