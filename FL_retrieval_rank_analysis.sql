select count(distinct requestUUID) as n_requests, count(*) as n_rows 
from `etsy-search-ml-dev.yzhang.rank_fx_data`
-- n_requests 3797020
-- n_rows 156769533


select platform_id, data_type_id, count(distinct requestUUID) as n_requests, count(*) as n_rows 
from `etsy-search-ml-dev.yzhang.rank_fx_data`
group by platform_id, data_type_id
order by platform_id, data_type_id


select platform_id, data_type_id, count(*)
from `etsy-search-ml-dev.yzhang.rank_fx_data`
where (
    rpc_blended_rank is null and fl_blended_rank is not null
)
or (
    rpc_blended_rank is not null and fl_blended_rank is null
)
group by platform_id, data_type_id
order by platform_id, data_type_id


select platform_id, data_type_id, count(*)
from `etsy-search-ml-dev.yzhang.rank_fx_data`
where (
    rpc_blended_rank is not null and fl_blended_rank is not null
)
and rpc_blended_rank != fl_blended_rank
group by platform_id, data_type_id
order by platform_id, data_type_id


select platform_id, data_type_id, count(*)
from `etsy-search-ml-dev.yzhang.rank_fx_data`
where (
    rpc_blended_rank is not null and fl_blended_rank is not null and rpc_blended_rank != fl_blended_rank
) 
or (
    rpc_neural_ir_rank is not null and fl_neural_ir_rank is not null and rpc_neural_ir_rank != fl_neural_ir_rank
)
or (
    rpc_xml_rank is not null and fl_xml_rank is not null and rpc_xml_rank != fl_xml_rank
)
or (
    rpc_solr_rank is not null and fl_solr_rank is not null and rpc_solr_rank != fl_solr_rank
)
or (
    rpc_xwalk_rank is not null and fl_xwalk_rank is not null and rpc_xwalk_rank != fl_xwalk_rank
)
or (
    rpc_grepsy_rank is not null and fl_grepsy_rank is not null and rpc_grepsy_rank != fl_grepsy_rank
)
group by platform_id, data_type_id
order by platform_id, data_type_id


with blended_diff as (
    select platform_id, data_type_id, fl_blended_rank - rpc_blended_rank as rank_diff
    from `etsy-search-ml-dev.yzhang.rank_fx_data`
    where (
        rpc_blended_rank is not null and fl_blended_rank is not null
        and rpc_blended_rank != fl_blended_rank
    )
)
select 
  platform_id, data_type_id, 
  min(rank_diff) as rd_min,
  APPROX_QUANTILES(rank_diff, 100)[OFFSET(25)] rd_p25,
  avg(rank_diff) as rd_mean,
  APPROX_QUANTILES(rank_diff, 100)[OFFSET(50)] rd_p50,
  APPROX_QUANTILES(rank_diff, 100)[OFFSET(75)] rd_p75,
  max(rank_diff) as rd_max,
from blended_diff
group by platform_id, data_type_id
order by platform_id, data_type_id
