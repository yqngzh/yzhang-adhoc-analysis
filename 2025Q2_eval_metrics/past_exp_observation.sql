-- Categorical Embeddings + Buyer360 SI
select 
  modelName,
  avg(metrics.purchase.ndcg48) as avg_pndcg48,  
  avg(metrics.purchase.ndcg10) as avg_pndcg10, 
  avg(metrics.purchase.dcgAttributedPrice10) as avg_ppdcg10
from `etsy-search-ml-prod.search_ranking.second_pass_eval`
where evalDate between date("2025-04-08") and date("2025-04-22")
and source in ("web_purchase", "boe_purchase")
and tags.userId > 0
and modelName in (
  "nrv2-us-intl-si", "nrv2-unif-emb-si"
)
group by modelName
order by modelName
-- modelName	avg_pndcg48	avg_pndcg10	avg_ppdcg10
-- nrv2-us-intl-si	0.5445684286374215	0.48196940979436331	3.25000760385303
-- nrv2-unif-emb-si	0.54521480804307665	0.4831772924626016	3.2551400930034879


-- US model w/ added intl features (SO)
select 
  modelName,
  avg(metrics.purchase.ndcg48) as avg_pndcg48,  
  avg(metrics.purchase.ndcg10) as avg_pndcg10, 
  avg(metrics.purchase.dcgAttributedPrice10) as avg_ppdcg10
from `etsy-search-ml-prod.search_ranking.second_pass_eval`
where evalDate between date("2025-03-12") and date("2025-03-27")
and source in ("web_purchase", "boe_purchase")
and (tags.userId = 0 or tags.userId is null)
and modelName in (
  "nrv2-semrel-uni-serve-tm-so", "nrv2-us-intl-so"
)
group by modelName
order by modelName
-- modelName	avg_pndcg48	avg_pndcg10	avg_ppdcg10
-- nrv2-semrel-uni-serve-tm-so	0.508844417576389	0.43307301064101728	3.1153643422151873
-- nrv2-us-intl-so	0.51276976838093224	0.43848579810518862	3.154519249432433


-- US model w/ added intl features (SI)
select 
  modelName,
  avg(metrics.purchase.ndcg48) as avg_pndcg48,  
  avg(metrics.purchase.ndcg10) as avg_pndcg10, 
  avg(metrics.purchase.dcgAttributedPrice10) as avg_ppdcg10
from `etsy-search-ml-prod.search_ranking.second_pass_eval`
where evalDate between date("2025-03-12") and date("2025-03-27")
and source in ("web_purchase", "boe_purchase")
and tags.userId > 0
and modelName in (
  "nrv2-no-borda-tm-si", "nrv2-us-intl-si"
)
group by modelName
order by modelName
-- modelName	avg_pndcg48	avg_pndcg10	avg_ppdcg10
-- nrv2-no-borda-tm-si	0.54116721866303763	0.47708650418095361	3.1516140712744729
-- nrv2-us-intl-si	0.54649418025109586	0.48449270124292321	3.2030539336047581



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
