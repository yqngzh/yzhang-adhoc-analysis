SELECT AVG(metrics.purchase.ndcg10), 
FROM `etsy-search-ml-dev.search_ranking.second_pass_eval`
WHERE
	modelName like "%nrv2-hchen%"
	AND evalDate="2025-08-02"
	AND source="web_purchase"

-- https://grafana.etsycloud.com/goto/sg-hl7_Hg?orgId=1
-- https://docs.google.com/document/d/1dZyqMg33QEI7cScfihnJz8PboVp3sGavU9q_aTElQOw/edit?usp=sharing