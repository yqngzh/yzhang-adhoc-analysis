DECLARE start_date DATE DEFAULT '2025-10-05';
DECLARE end_date DATE DEFAULT '2025-10-05';
DECLARE config_flags ARRAY<STRING> DEFAULT ['ranking/search.mmx.2025_q3.exact_match_semrel_v3_contextual_us'];

with bucketing as (
 	select
        experiment_id AS config_flag, 
        bucketing_id,
        variant_id, 
        bucketing_ts
    from `etsy-data-warehouse-prod.catapult_unified.bucketing_period` 
    where _date = end_date 
    and experiment_id in unnest(config_flags)
),

mmx_requests_in_exp as (
    select
        date(timestamp_millis(beacon.timestamp)) as _date, 
        b.visit_id,
        bucketing_id,
        variant_id,
        (select value from unnest(beacon.properties.key_value) where key = 'query') as query,
        (select value from unnest(beacon.properties.key_value) where key = 'mmx_request_uuid') as mmx_request_uuid,
    from `etsy-visit-pipe-prod.canonical.visit_id_beacons` b
    join bucketing 
    on b.beacon.browser_id = bucketing.bucketing_id 
    and timestamp_millis(b.beacon.timestamp) >= bucketing.bucketing_ts
    where
    date(_partitiontime) between start_date and end_date
    and beacon.event_name = 'search'
    and (select value from unnest(beacon.properties.key_value) where key = 'search_placement') in ('wsg', 'allsr')
),

rpc_logs_all as (
    select 
        response.mmxRequestUUID as mmx_request_uuid,
        OrganicRequestMetadata.candidateSources candidateSources
    FROM `etsy-searchinfra-gke-prod-2.thrift_mmx_listingsv2search_search.rpc_logs_*`
    WHERE request.OPTIONS.cacheBucketId LIKE "live%"
    AND request.options.csrOrganic
    AND request.options.searchPlacement IN ('wsg', 'allsr', 'wmg')
    AND request.query <> ''
    AND request.offset = 0
    AND DATE(queryTime) between start_date and end_date
    AND request.options.interleavingConfig IS NULL
    AND NOT EXISTS (
      SELECT * FROM UNNEST(request.context)
      WHERE key = "req_source" AND value = "bot"
    )
),

rpc_logs_in_exp as (
    select distinct
        variant_id,
        mmx_request_uuid,
        candidateSources
    from mmx_requests_in_exp
    join rpc_logs_all using(mmx_request_uuid)
),

stage_counts AS (
    SELECT
        variant_id,
        mmx_request_uuid,
        cs.stage,
        ARRAY_LENGTH(cs.listingIds) AS candidate_count,
    FROM rpc_logs_in_exp, UNNEST(rpc_logs_in_exp.candidateSources) cs
    WHERE cs.stage IN ('POST_FILTER', 'POST_SEM_REL_FILTER', 'POST_BORDA', 'RANKING', 'MO_LASTPASS')
    AND cs.listingIds IS NOT NULL
    AND ARRAY_LENGTH(cs.listingIds) > 0
),

avg_stage_candidate_size AS (
    SELECT 
        variant_id,
        stage,
        COUNT(DISTINCT mmx_request_uuid) AS n_requests,
        AVG(candidate_count) AS avg_candidate_count
    FROM stage_counts
    GROUP BY variant_id, stage
)
SELECT * FROM avg_stage_candidate_size
ORDER BY stage, variant_id


-- 10-01 
-- off	POST_SEM_REL_FILTER	56259	1481.3699496969361
-- on	POST_SEM_REL_FILTER	56640	1485.7173375706229
-- 10-02
-- off	POST_SEM_REL_FILTER	2286530	1498.0940801126571
-- on	POST_SEM_REL_FILTER	2289881	1497.7487437120024
-- 10-03
-- off	POST_SEM_REL_FILTER	2276159	1494.7648499072409
-- on	POST_SEM_REL_FILTER	2284563	1495.8249652121656
-- 10-04
-- off	POST_SEM_REL_FILTER	2362360	1496.0426086625273
-- on	POST_SEM_REL_FILTER	2358875	1496.4470991468422
-- 10-05
-- off	POST_SEM_REL_FILTER	2650349	1488.7051388326602
-- on	POST_SEM_REL_FILTER	2659446	1491.0036248150866
-- 10-06
-- off	POST_SEM_REL_FILTER	2636665	1495.4158617040846
-- on	POST_SEM_REL_FILTER	2638476	1495.2887564639705
-- 10-07
-- off	POST_SEM_REL_FILTER	2566273	1490.3404957305831
-- on	POST_SEM_REL_FILTER	2563253	1490.1327641087266	
-- 10-08
-- off	POST_SEM_REL_FILTER	2494678	1498.0661379945659
-- on	POST_SEM_REL_FILTER	2510776	1499.4349603469161
-- 10-09
-- 
-- 
-- 10-10
-- 10-11
-- 10-12