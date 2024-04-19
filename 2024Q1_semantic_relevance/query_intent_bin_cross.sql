with qis AS (
    SELECT 
        query_raw query,
        CASE 
            WHEN class_id = 0 THEN 'broad' 
            WHEN class_id = 1 THEN 'direct_unspecified'
            WHEN class_id = 2 THEN 'direct_specified' 
        END AS qisClass
    FROM `etsy-search-ml-prod.mission_understanding.qis_scores`
),
qlm AS (
    SELECT query_raw query, _date date, bin 
    FROM `etsy-batchjobs-prod.snapshots.query_level_metrics_raw`
    WHERE _date = DATE("2024-04-15")
),
merged as (
    select qis.query, qisClass, qlm.bin as queryBin
    from qis 
    join qlm
    on qis.query = qlm.query
)
select queryBin, qisClass, count(*) as qCount
from merged
group by queryBin, qisClass
order by queryBin, qisClass