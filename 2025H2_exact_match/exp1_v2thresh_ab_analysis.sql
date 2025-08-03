-- semrel metrics
with semrel_results as (
    SELECT variantName, guid, query, platform, userLanguage, listingId, classId
    FROM `etsy-data-warehouse-prod.search.sem_rel_hydrated_daily_requests_per_experiment` s
    left JOIN `etsy-data-warehouse-prod.search.sem_rel_query_listing_metrics_per_experiment` m
    USING (tableUUID, date)
    WHERE configFlag = "ranking/isearch.exact_match_unify_with_v2_loc"
    -- AND s.date = "2025-08-01"
    AND pageNum is not null
    -- AND modelName = "v2-deberta-v3-large-tad"
    AND modelName = "v3-finetuned-llama-8b"
),
agg_results as (
    select variantName, guid, sum(if(classId=1, 1, 0)) as n_irrelevant, count(*) as n_total
    from semrel_results
    group by variantName, guid
)
select variantName, avg(n_irrelevant / n_total)
from agg_results
group by variantName


create or replace