---- How candidate set size changes over stages
declare date_string default "2024-12-01";
declare min_results default 68;
declare platform default "boe";

with allRequests as (
    select
        RequestIdentifiers.etsyRequestUUID as etsyUUID,
        response.mmxRequestUUID as mmxRequestUUID,
        DATE(queryTime) query_date,
        (SELECT COUNTIF(stage='POST_FILTER') FROM UNNEST(OrganicRequestMetadata.candidateSources)) nPostFilterSources,
        (SELECT COUNTIF(stage='POST_BORDA') FROM UNNEST(OrganicRequestMetadata.candidateSources)) nPostBordaSources,
        (SELECT COUNTIF(stage='POST_SEM_REL_FILTER') FROM UNNEST(OrganicRequestMetadata.candidateSources)) nPostSemrelSources,
        (SELECT COUNTIF(stage='RANKING') FROM UNNEST(OrganicRequestMetadata.candidateSources)) nRankingSources,
        (SELECT COUNTIF(stage='MO_LASTPASS') FROM UNNEST(OrganicRequestMetadata.candidateSources)) nLastpassSources,
        COALESCE((SELECT NULLIF(query, '') FROM UNNEST(request.filter.query.translations) WHERE language = 'en'), NULLIF(request.query, '')) query,
        request.options.userCountry userCountry,
        (SELECT
            CASE
                WHEN value = 'web' THEN 'web'
                WHEN value = 'web_mobile' THEN 'mweb'
                WHEN value IN ('etsy_app_android', 'etsy_app_ios', 'etsy_app_other') THEN 'boe'
                ELSE value
            END
            FROM unnest(request.context)
            WHERE key = "req_source"
        ) as requestSource,
        OrganicRequestMetadata.candidateSources candidateSources
    FROM `etsy-searchinfra-gke-prod-2.thrift_mmx_listingsv2search_search.rpc_logs_*`
    WHERE request.OPTIONS.cacheBucketId LIKE "live%"
    AND request.options.csrOrganic
    AND request.options.searchPlacement IN ('wsg', 'allsr')
    AND request.query <> ''
    AND request.offset = 0
    AND DATE(queryTime) = DATE(date_string)
    AND request.options.interleavingConfig IS NULL
    AND response.count >= min_results
    AND NOT EXISTS (
      SELECT * FROM UNNEST(request.context)
      WHERE key = "req_source" AND value = "bot"
    )
),
validRequests AS (
    SELECT *
    FROM allRequests
    WHERE requestSource = platform
    AND nLastPassSources=1
    AND nRankingSources=1
    AND nPostFilterSources>=1
    AND (nPostBordaSources>=1 OR nPostSemrelSources>=1)
    AND userCountry = 'US'
    ----  US only
),
candidate_set_size as (
    select 
        etsyUUID, mmxRequestUUID, rootUUID, query, 
        cs.source as retrieval_engine, 
        array_length(cs.listingIds) as n_candidates, 
    from validRequests, UNNEST(candidateSources) cs
    where cs.stage = "POST_FILTER"
)
select 
  retrieval_engine,
  min(n_candidates) as min_n,
  APPROX_QUANTILES(n_candidates, 100)[OFFSET(25)] AS percentile_25,
  avg(n_candidates) as avg_n,
  APPROX_QUANTILES(n_candidates, 100)[OFFSET(50)] AS median_n,
  APPROX_QUANTILES(n_candidates, 100)[OFFSET(75)] AS percentile_75,
  APPROX_QUANTILES(n_candidates, 100)[OFFSET(90)] AS percentile_90,
  max(n_candidates) as max_n,
from candidate_set_size
group by retrieval_engine



---- how much semrel filters
declare date_string default "2024-12-01";
declare min_results default 88;
declare platform default "web";

with allRequests as (
    select
        RequestIdentifiers.etsyRequestUUID as etsyUUID,
        response.mmxRequestUUID as mmxRequestUUID,
        DATE(queryTime) query_date,
        (SELECT COUNTIF(stage='POST_FILTER') FROM UNNEST(OrganicRequestMetadata.candidateSources)) nPostFilterSources,
        (SELECT COUNTIF(stage='POST_BORDA') FROM UNNEST(OrganicRequestMetadata.candidateSources)) nPostBordaSources,
        (SELECT COUNTIF(stage='POST_SEM_REL_FILTER') FROM UNNEST(OrganicRequestMetadata.candidateSources)) nPostSemrelSources,
        (SELECT COUNTIF(stage='RANKING') FROM UNNEST(OrganicRequestMetadata.candidateSources)) nRankingSources,
        (SELECT COUNTIF(stage='MO_LASTPASS') FROM UNNEST(OrganicRequestMetadata.candidateSources)) nLastpassSources,
        COALESCE((SELECT NULLIF(query, '') FROM UNNEST(request.filter.query.translations) WHERE language = 'en'), NULLIF(request.query, '')) query,
        request.options.userCountry userCountry,
        (SELECT
            CASE
                WHEN value = 'web' THEN 'web'
                WHEN value = 'web_mobile' THEN 'mweb'
                WHEN value IN ('etsy_app_android', 'etsy_app_ios', 'etsy_app_other') THEN 'boe'
                ELSE value
            END
            FROM unnest(request.context)
            WHERE key = "req_source"
        ) as requestSource,
        OrganicRequestMetadata.candidateSources candidateSources
    FROM `etsy-searchinfra-gke-prod-2.thrift_mmx_listingsv2search_search.rpc_logs_*`
    WHERE request.OPTIONS.cacheBucketId LIKE "live%"
    AND request.options.csrOrganic
    AND request.options.searchPlacement IN ('wsg', 'allsr')
    AND request.query <> ''
    AND request.offset = 0
    AND DATE(queryTime) = DATE(date_string)
    AND request.options.interleavingConfig IS NULL
    AND response.count >= min_results
    AND NOT EXISTS (
      SELECT * FROM UNNEST(request.context)
      WHERE key = "req_source" AND value = "bot"
    )
),
validRequests AS (
    SELECT *
    FROM allRequests
    WHERE requestSource = platform
    AND nLastPassSources=1
    AND nRankingSources=1
    AND nPostFilterSources>=1
    AND (nPostBordaSources>=1 OR nPostSemrelSources>=1)
    AND userCountry = 'US'
    ----  US only
),
reqCandidateSizes AS (
    SELECT
        etsyUUID,
        mmxRequestUUID,
        query,
        array_length(
            ARRAY(
                SELECT STRUCT(
                    listing_id AS listingId,
                    idx AS postBordaRank
                )
                FROM UNNEST(candidateSources) cs,
                    UNNEST(cs.listingIds) AS listing_id WITH OFFSET idx
                WHERE cs.stage = "POST_BORDA"
            )
        ) as post_borda_n_listings,
        array_length(
            ARRAY(
                SELECT STRUCT(
                    listing_id AS listingId,
                    idx AS postSemrelRank
                )
                FROM UNNEST(candidateSources) cs,
                    UNNEST(cs.listingIds) AS listing_id WITH OFFSET idx
                WHERE cs.stage = "POST_SEM_REL_FILTER"
            )
        ) as post_semrel_n_listings,
    FROM validRequests
), 
n_candidates_diff as (
  select post_borda_n_listings - post_semrel_n_listings as n_semrel_filtered
  from reqCandidateSizes
  where post_borda_n_listings > post_semrel_n_listings
)
select 
  min(n_semrel_filtered) as min_n,
  APPROX_QUANTILES(n_semrel_filtered, 100)[OFFSET(25)] AS percentile_25,
  avg(n_semrel_filtered) as avg_n,
  APPROX_QUANTILES(n_semrel_filtered, 100)[OFFSET(50)] AS median_n,
  APPROX_QUANTILES(n_semrel_filtered, 100)[OFFSET(75)] AS percentile_75,
  APPROX_QUANTILES(n_semrel_filtered, 100)[OFFSET(90)] AS percentile_90,
  max(n_semrel_filtered) as max_n,
from n_candidates_diff

-- select count(*)
-- from reqCandidateSizes
-- where post_borda_n_listings > 0

-- select count(*)
-- from n_candidates_diff
-- where n_semrel_filtered > 0

-- web
-- 3119087 requests
-- 3118692 nPostSemrel >= 1,  3114863 nPostBorda >= 1
-- 2277136 (73%) n_semrel_filtered > 0 (some listings were filtered)
-- median 20, average 75.7, 75% 83, max 2111
-- boe
-- 6547834 requests
-- 6546958 nPostSemrel >= 1, 6544290 nPostBorda >= 1
-- 5332460 (81.4%) n_semrel_filtered > 0 (some listings were filtered)
-- median 28, average 80.4, 75% 101, max 1982




---- Human annotation irrelevant / partial / relevant
with anno_v2_irrelevant as (
  SELECT 
    query, qisClass, listingId, gold_label, 
    row_number() over (partition by qisClass order by rand()) as row_num 
  FROM `etsy-search-ml-dev.yzhang.sem_rel_human_annotation_v2`
  where gold_label = 'not_relevant'
)
select * from anno_v2_irrelevant
where row_num <= 20



---- Sample organic query listing pairs and their impressions, clicks, purchases
--- CVR = % percent of unique browsers with a backend cart payment event
-- platform = boe, desktop, mobile_web

-- on a certain day, a certain platform, browsers that purchased any query listing pair
declare date_string default "2024-11-04";
declare platform default "desktop";

create or replace table `etsy-search-ml-dev.yzhang.organic_impression_semrel_desktop_1104` as (
    with organic_impressed as (
        select _date, browser_id, visit_id, query, listing_id, page_number, position, sum(n_seen) as n_impressed, sum(n_clicked) as n_clicked, sum(purchased_same_day_last_click) as n_purchased
        from `etsy-data-warehouse-prod.rollups.organic_impressions`
        where _date = date(date_string)
        and placement = "search"
        and platform = platform
        and page_number is not null
        and page_number in ('1', '2', '3', '4')
        group by _date, browser_id, visit_id, query, listing_id, page_number, position
    ),
    visit_id_to_guid_mapping as (
        select distinct visit_id, beacon.guid
        from `etsy-visit-pipe-prod.canonical.visit_id_beacons`
        where date(_PARTITIONTIME) = date(date_string)
        and beacon.event_name = "search"
        -- guid distinct
    ),
    organic_impressed_add_guid as (
        select organic_impressed.*, guid
        from organic_impressed
        inner join visit_id_to_guid_mapping
        on organic_impressed.visit_id = visit_id_to_guid_mapping.visit_id
    ),
    semrel_labels as (
        SELECT guid, query, listingId, classId, softmaxScores
        FROM `etsy-data-warehouse-prod.search.sem_rel_hydrated_daily_requests`
        JOIN `etsy-data-warehouse-prod.search.sem_rel_query_listing_metrics`
        USING (tableUUID, date)
        where date = date(date_string)
        and modelName = 'v2-deberta-v3-large-tad'
    )
    select oiag.*, semrel.classId, semrel.softmaxScores
    from organic_impressed_add_guid oiag
    inner join semrel_labels semrel
    on oiag.guid = semrel.guid
    and oiag.listing_id = semrel.listingId
    and oiag.query = LEFT(semrel.query, 35)
)

SELECT count(*)
FROM `etsy-ml-systems-prod.feature_bank_v2.query_feature_bank_most_recent`
where queryIntentFeatures_queryIntentSpecScoreV2 is not null
-- SELECT count(distinct searchQuery) FROM `etsy-data-warehouse-prod.arizona.query_entity_features`
-- 3102938570
-- qis 873782611 
-- queryEntity 14105857 (cover ~68% high GMS query)