select
sum(case when user_id is not null then total_gms end) / sum(total_gms) as si_gms,
count(distinct case when user_id is not null then visit_id end) / count(distinct visit_id) as si_traffic
from `etsy-data-warehouse-prod.weblog.visits`
where _date >= current_date - interval 8 day
group by all