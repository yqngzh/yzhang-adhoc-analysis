DECLARE start_date DATE DEFAULT "2025-05-23";
DECLARE end_date DATE DEFAULT "2025-06-04";
DECLARE experiment_name STRING DEFAULT "ranking/search.mmx.2025_q2.nrv2_unified_ranking_try2";
DECLARE study_date DATE DEFAULT "2025-05-30";

select 
  modelName, 
  avg(metrics.purchase.ndcg10) as avg_pndcg10, 
  avg(metrics.purchase.ndcg48) as avg_pndcg48, 
  avg(metrics.purchase.dcgAttributedPrice10) as avg_ppdcg10,
  avg(metrics.purchase.dcgAttributedPrice48) as avg_ppdcg48
from `etsy-search-ml-prod.search_ranking.second_pass_eval`
where evalDate between date(start_date) and date(end_date)
and source in ("web_purchase", "boe_purchase")
and modelName in (

)
group by modelName
order by modelName



with launches as (
  select 
    config_flag,
    max(launch_id) launch_id -- launch_id for most recent boundary
  from `etsy-data-warehouse-prod.etsy_atlas.catapult_launches` 
  where team = 'Search Ranking'
  group by 1
),
configs AS (
  select
    e.experiment_id config_flag,
    l.launch_id,
    e.boundary_start_ts,
    date(boundary_start_ts) as start_date,
    max(e._date) as end_date,
  FROM launches l
  join `etsy-data-warehouse-prod.catapult_unified.experiment` e
    on l.config_flag = e.experiment_id
    and e._date >= '2024-06-01'
  group by 1, 2, 3
  qualify row_number() over(partition by e.experiment_id order by boundary_start_ts desc) = 1 -- most recent boundary only 
),
results as (
select 
  config_flag,
  start_date, 
  end_date,
  metric_variant_name as variant_id,
  if(metric_display_name like '%purchase_NDCG%', 'purchase NDCG', metric_display_name) metric, 
  relative_change, 
  p_value, 
  is_significant
from `etsy-data-warehouse-prod.catapult.results_metric_day` r
join configs c
  on c.launch_id = r.launch_id
  and c.end_date = r._date
where 
  _date >= '2024-06-01'
  and segmentation = 'any'
  and segment = 'all'
  and metric_display_name in (
    'Conversion Rate',
    'GMS per Unit',
    -- 'Winsorized AC*V',
    'Mean purchase_NDCG / rich_search_events_w_purchase (event level)'
  )
)
select * 
from results 
order by start_date desc, config_flag, variant_id, metric
-- online_metrics.csv
-- copied from Maggie's work https://docs.google.com/spreadsheets/d/1ABbUqhPFxfPwnzczwha3IEkCjbXX8K-BWmJzktngEoo/edit?usp=sharing
