---- analyze past experiments
-- retrieval web: https://atlas.etsycorp.com/catapult/1280417515477
---- ranking/search.mmx.2024_q3.semrel_filtering_web_v2
---- 2024-07-09, 2024-07-24
-- retrieval boe: https://atlas.etsycorp.com/catapult/1280417995635
---- ranking/search.mmx.2024_q3.semrel_filtering_boe_v2
---- 2024-07-09, 2024-07-24
-- international ranking: https://atlas.etsycorp.com/catapult/1300821193551 
---- ranking/isearch.nr_loc_sem_rel
---- 2024-09-09, 2024-09-17

DECLARE start_date DATE DEFAULT "2024-07-09";
DECLARE end_date DATE DEFAULT "2024-07-24";
DECLARE config_flag_param STRING DEFAULT "ranking/search.mmx.2024_q3.semrel_filtering_web_v2";

create or replace table `etsy-search-ml-dev.yzhang.retrieval_filterv2_web` as (
    with ab_first_bucket as (
        SELECT 
            bucketing_id, bucketing_id_type, variant_id, bucketing_ts
        FROM `etsy-data-warehouse-prod.catapult_unified.bucketing_period`
        WHERE _date = end_date
        AND experiment_id = config_flag_param
    ),
    first_bucket_segments_unpivoted as (
        SELECT 
            bucketing_id, variant_id, event_id, event_value
        FROM `etsy-data-warehouse-prod.catapult_unified.aggregated_segment_event`
        WHERE _date = end_date
        AND experiment_id = config_flag_param
        AND event_id IN (
            "buyer_segment",
            "canonical_region"
        )
    ),
    first_bucket_segments as (
        SELECT *
        FROM first_bucket_segments_unpivoted
        PIVOT(
            MAX(event_value)
            FOR event_id IN (
                "buyer_segment",
                "canonical_region"
            )
        )
    ),
    events as (
        SELECT *
        FROM UNNEST([
            "backend_cart_payment", -- conversion rate
            "total_winsorized_gms", -- winsorized acbv
            "organic_search_click",
            "organic_search_purchase",
            "organic_search_tail_query_click",
            "organic_search_tail_query_purchase"
        ]) AS event_id
    ),
    events_per_unit as (
        SELECT
            bucketing_id, variant_id, event_id, event_value
        FROM `etsy-data-warehouse-prod.catapult_unified.aggregated_event_func`(start_date, end_date)
        JOIN events USING (event_id)
        WHERE experiment_id = config_flag_param   
    )
    SELECT
        bucketing_id,
        variant_id,
        event_id,
        COALESCE(event_value, 0) AS event_count,
        buyer_segment,
        canonical_region,
    FROM ab_first_bucket
    CROSS JOIN events
    LEFT JOIN events_per_unit
    USING(bucketing_id, variant_id, event_id)
    JOIN first_bucket_segments
    USING(bucketing_id, variant_id)
);

SELECT
    variant_id,
    AVG(IF(event_count = 0, 0, 1)) AS percent_units_with_event,
FROM `etsy-search-ml-dev.yzhang.retrieval_filterv2_web`
where event_id = "organic_search_tail_query_click"
GROUP BY variant_id
ORDER BY variant_id;



---- coverage of QEE and query taxo bias towards top - head queries
with qlm AS (
    select distinct query_raw as query, bin as queryBin 
    from `etsy-data-warehouse-prod.rollups.query_level_metrics_raw`
),
qisv3 AS (
    select query_raw query,
    CASE 
    WHEN prediction = 0 THEN 'broad' 
    WHEN prediction = 1 THEN 'direct_unspecified'
    WHEN prediction = 2 THEN 'direct_specified' 
    END as qisClass    from `etsy-search-ml-prod.mission_understanding.qis_scores_v3`
),
qtcv5 as (
    select distinct
    coalesce(s.query, b.query) as query,
    coalesce(s.full_path, b.full_path) as queryTaxoFullPath,
    from `etsy-data-warehouse-prod.mission_understanding.query_taxonomy_seller` s
    full outer join `etsy-data-warehouse-prod.mission_understanding.query_taxonomy_buyer` b
    using(query)
),
qee_raw AS (
    select distinct
    searchQuery as query,
    entities,
    from `etsy-data-warehouse-prod.arizona.query_entity_features`
),
qee AS (
  select *
  from qee_raw
  QUALIFY ROW_NUMBER() OVER (PARTITION BY query ORDER BY RAND()) = 1 
),
merged as (
    select 
      qisv3.query,
      queryBin,
      qisClass,
      queryTaxoFullPath,
      split(queryTaxoFullPath, ".")[offset(0)] as queryTaxoTop,
      entities
    from qisv3
    left join qlm using (query)
    left join qtcv5 using (query)
    left join qee using (query)
)
-- select queryBin, queryTaxoTop, count(*) as n_queries
-- from merged
-- group by queryBin, queryTaxoTop
-- order by queryBin, queryTaxoTop
, res as (
    select queryBin, if(entities is null, "missing", "has_entities") as has_entity
    from merged
)
select queryBin, has_entity, count(*)
from res
group by queryBin, has_entity
order by queryBin, has_entity


with qlm as (
    select distinct query_raw as query, bin as queryBin 
    from `etsy-data-warehouse-prod.rollups.query_level_metrics_raw`
),
qisv3 AS (
    select query_raw query,
    CASE 
    WHEN prediction = 0 THEN 'broad' 
    WHEN prediction = 1 THEN 'direct_unspecified'
    WHEN prediction = 2 THEN 'direct_specified' 
    END as qisClass    from `etsy-search-ml-prod.mission_understanding.qis_scores_v3`
),
qee as (
    select distinct input.query, "has_entity" as entities
    from `etsy-search-ml-prod.mission_understanding.query_entity_extraction_v2_canonical_values`
),
merged as (
    select 
      qisv3.query,
      queryBin,
      entities
    from qisv3
    left join qlm using (query)
    left join qee using (query)
), 
res as (
    select queryBin, ifnull(entities, "missing") as has_entity
    from merged
)
select queryBin, has_entity, count(*)
from res
group by queryBin, has_entity
order by queryBin, has_entity
