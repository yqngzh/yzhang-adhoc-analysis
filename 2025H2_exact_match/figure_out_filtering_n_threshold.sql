DECLARE start_date DATE DEFAULT '2025-09-28';
DECLARE end_date DATE DEFAULT '2025-09-28';
DECLARE config_flags ARRAY<STRING> DEFAULT ['ranking/isearch.nir_unify_intl'];


create or replace table `etsy-sr-etl-prod.yzhang.global_nir_semrel_filter` as (
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

    beacon as (
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

    rpc AS (
        SELECT DISTINCT
            response.mmxRequestUUID as mmx_request_uuid,
            request.query AS query,
            OrganicRequestMetadata.candidateSources AS candidateSources,
        FROM `etsy-searchinfra-gke-prod-2.thrift_mmx_listingsv2search_search.rpc_logs_*`
        WHERE date(queryTime) BETWEEN start_date AND end_date
        AND request.options.cacheBucketId LIKE 'live%'
        AND request.query <> ''
        AND request.options.csrOrganic
        AND request.options.searchPlacement IN ('wsg', "wmg", "allsr")
        AND request.offset = 0
        AND request.options.interleavingConfig IS NULL
        AND OrganicRequestMetadata IS NOT NULL
        AND EXISTS (
            SELECT 1 FROM UNNEST(OrganicRequestMetadata.candidateSources) cs
            WHERE cs.stage IN ('POST_FILTER', 'POST_SEM_REL_FILTER', 'POST_BORDA', 'RANKING', 'MO_LASTPASS')
                AND cs.listingIds IS NOT NULL
                AND ARRAY_LENGTH(cs.listingIds) > 0
        )
        AND NOT EXISTS (
            SELECT 1 FROM UNNEST(request.context)
            WHERE key = 'req_source' AND value = 'bot'
        )
        AND response.mmxRequestUUID IS NOT NULL
    ),

    rpc_selected as (
        select distinct
            b.*,
            r.candidateSources
        from beacon b
        join rpc r using (mmx_request_uuid, query)
    )

    select * from rpc_selected
)


with tmp as (
  select distinct variant_id, mmx_request_uuid, candidateSources
  from `etsy-sr-etl-prod.yzhang.global_nir_semrel_filter`
)
select variant_id, count(*)
from tmp
group by variant_id

-- on	1579440
-- off	1588028


with requests as (
   select distinct variant_id, mmx_request_uuid, candidateSources
   from `etsy-sr-etl-prod.yzhang.global_nir_semrel_filter`
),

stage_counts AS (
    SELECT distinct
        variant_id,
        mmx_request_uuid,
        cs.stage,
        ARRAY_LENGTH(cs.listingIds) AS candidate_count,
    FROM requests, UNNEST(requests.candidateSources) cs
    WHERE cs.stage IN ('POST_FILTER', 'POST_SEM_REL_FILTER', 'POST_BORDA', 'RANKING', 'MO_LASTPASS')
    AND cs.listingIds IS NOT NULL
    AND ARRAY_LENGTH(cs.listingIds) > 0
),

semrel_filtered_count as (
    select 
        variant_id,
        mmx_request_uuid,
        SUM(CASE WHEN stage = 'POST_FILTER' THEN candidate_count END) n_post_filter,
        SUM(CASE WHEN stage = 'POST_SEM_REL_FILTER' THEN candidate_count END) n_post_sem_rel_filter,
        SUM(CASE WHEN stage = 'POST_BORDA' THEN candidate_count END) n_post_borda,
        SUM(CASE WHEN stage = 'RANKING' THEN candidate_count END) n_ranking,
        SUM(CASE WHEN stage = 'MO_LASTPASS' THEN candidate_count END) n_mo_lastpass,
        SUM(CASE WHEN stage = 'POST_FILTER' THEN candidate_count END)
            - SUM(CASE WHEN stage = 'POST_SEM_REL_FILTER' THEN candidate_count END) as n_filtered
    from stage_counts
    group by variant_id, mmx_request_uuid
)

select 
  variant_id, 
  avg(n_post_filter) avg_post_filter,
  avg(n_post_sem_rel_filter) avg_post_semrel_filter,
  avg(n_post_borda) avg_post_borda,
  avg(n_ranking) avg_ranking,
  avg(n_mo_lastpass) avg_lastpass,
  avg(n_filtered) avg_filtered,
  count(*) n_requests
from semrel_filtered_count
group by variant_id

