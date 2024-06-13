DECLARE start_date DATE DEFAULT '2024-05-08';
DECLARE end_date DATE DEFAULT '2024-05-20';
DECLARE sample_rate FLOAT64 DEFAULT 0.01;
DECLARE listingPerPage INT64 DEFAULT 48;


-- sample request over experiment window
create or replace table `etsy-data-warehouse-dev.search.sr-sem-rel-v1-requests` as (
    with allRequests as (
        SELECT
            (SELECT COUNTIF(stage='MO_LASTPASS') FROM UNNEST(OrganicRequestMetadata.candidateSources)) nLastpassSources,
            (SELECT COUNTIF(stage='POST_FILTER') FROM UNNEST(OrganicRequestMetadata.candidateSources)) nPostFilterSources,
            (SELECT COUNTIF(stage='POST_BORDA') FROM UNNEST(OrganicRequestMetadata.candidateSources)) nPostBordaSources,
            request.query AS query,
            request.options.locale userLocale,
            DATE(queryTime) date,
            RequestIdentifiers.etsyRequestUUID as etsyUUID,
            response.mmxRequestUUID as mmxRequestUUID,
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
        AND request.query <> ''
        AND request.offset = 0
        AND DATE(queryTime) between start_date and end_date
        AND request.options.interleavingConfig IS NULL
        AND response.count >= 88
    ), 
    validRequests AS (
        SELECT * FROM allRequests 
        WHERE nLastPassSources=1
        AND nPostFilterSources>=1
        AND nPostBordaSources>=1
        AND userLocale = "US"
    )
    SELECT * FROM validRequests
    WHERE RAND() < sample_rate
);




-- web
create or replace table `etsy-data-warehouse-dev.search.sr-sem-rel-v1-web-ab` as (
    with bucketing as (
        select
            bucketing_id,
            variant_id,
            bucketing_ts,
        from `etsy-data-warehouse-prod.catapult_unified.bucketing_period`
        where _date between start_date and end_date
        and experiment_id = "ranking/search.mmx.2024_q2.nrv2_sem_rel_v1_web_try2"
    ),
    exp_visits as (
        select
            b.*,
            v.visit_id,
        from bucketing b
        join `etsy-data-warehouse-prod.weblog.visits` v
            on b.bucketing_id = v.browser_id
            and TIMESTAMP_TRUNC(bucketing_ts, SECOND) <= v.end_datetime
        where v._date between start_date and end_date
    ),
    beacon AS (
        select distinct
            beacon.guid,
            visit_id,
            date(_PARTITIONTIME) as date,
            (SELECT value FROM UNNEST(beacon.properties.key_value) WHERE key = 'mmx_request_uuid') AS mmxRequestUUID,
            (select value from unnest(beacon.properties.key_value) where key = 'query') as beacon_query
        from `etsy-visit-pipe-prod.canonical.visit_id_beacons`
        where date(_PARTITIONTIME) between start_date and end_date
        and beacon.event_name = 'search'
    ),
    exp_join_beacon as (
        select
            ev.variant_id,
            ev.visit_id,
            beacon.guid,
            beacon.mmxRequestUUID,
            beacon.beacon_query,
            beacon.date
        from exp_visits ev
        join beacon
        on ev.visit_id = beacon.visit_id
    )
    select distinct *
    from exp_join_beacon
);


create or replace table `etsy-data-warehouse-dev.search.sr-sem-rel-v1-web-results` as (
    with reqsSample as (
        select 
            exp_data.*,
            r.query,
            r.etsyUUID,
            r.requestSource,
            r.candidateSources
        from `etsy-data-warehouse-dev.search.sr-sem-rel-v1-web-ab` exp_data
        join `etsy-data-warehouse-dev.search.sr-sem-rel-v1-requests` r
        on exp_data.mmxRequestUUID = r.mmxRequestUUID
        and exp_data.date = r.date
        where r.requestSource in ('web', 'mweb')
    ),
    lfb AS (
        SELECT 
            key listingId, 
            verticaListings_taxonomyPath listingTaxo,
            NULLIF(verticaListings_title, "") listingTitle,
            verticaListings_description  listingDescription,
            verticaListings_tags listingTags,
        FROM `etsy-ml-systems-prod.feature_bank_v2.listing_feature_bank_most_recent`
    ),
    qis AS (
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
        WHERE _date BETWEEN start_date and end_date
    ),
    qfb AS (
        SELECT key query, queryTaxoClassification_taxoPath queryTaxo
        FROM `etsy-ml-systems-prod.feature_bank_v2.query_feature_bank_most_recent`
    ),
    queryHydratedRequests AS (
        SELECT
            variant_id,
            etsyUUID,
            mmxRequestUUID,
            guid,
            visit_id,
            query,
            beacon_query,
            REGEXP_CONTAINS(query, '(\?i)\\bgift|\\bfor (\\bhim|\\bher|\\bmom|\\bdad|\\bmother|\\bfather|\\bdaughter|\\bson|\\bwife|\\bhusband|\\bpartner|\\baunt|\\buncle|\\bniece|\\bnephew|\\bfiance|\\bcousin|\\bin law|\\bboyfriend|\\bgirlfriend|\\bgrand|\\bfriend|\\bbest friend)') isGift,
            IFNULL(qlm.bin, 'novel') queryBin,
            IFNULL(qis.qisClass, 'missing') qisClass,
            queryTaxo,
            requestSource platform,
            reqsSample.date,
            ARRAY_CONCAT(
                ARRAY(
                    SELECT STRUCT(listing_id AS listingId, idx AS rankingRank, CAST(NULL AS INT64) AS retrievalRank, CAST(NULL AS STRING) AS retrievalSrc, CAST(NULL AS INT64) AS bordaRank, 1 AS pageNum)
                    FROM UNNEST(candidateSources) cs,
                        UNNEST(cs.listingIds) AS listing_id WITH OFFSET idx
                    WHERE cs.stage = "MO_LASTPASS"
                    AND idx < listingPerPage
                ),
                ARRAY(
                    SELECT STRUCT(listing_id AS listingId, idx AS rankingRank, CAST(NULL AS INT64) AS retrievalRank, CAST(NULL AS STRING) AS retrievalSrc, CAST(NULL AS INT64) AS bordaRank, 2 AS pageNum)
                    FROM UNNEST(candidateSources) cs,
                        UNNEST(cs.listingIds) AS listing_id WITH OFFSET idx
                    WHERE cs.stage = "MO_LASTPASS"
                    AND idx >= listingPerPage AND idx < listingPerPage * 2
                    ORDER BY RAND()
                    LIMIT 10
                ),
                ARRAY(
                    SELECT STRUCT(listing_id AS listingId, idx AS rankingRank, CAST(NULL AS INT64) AS retrievalRank, CAST(NULL AS STRING) AS retrievalSrc, CAST(NULL AS INT64) AS bordaRank, 3 AS pageNum)
                    FROM UNNEST(candidateSources) cs,
                        UNNEST(cs.listingIds) AS listing_id WITH OFFSET idx
                    WHERE cs.stage = "MO_LASTPASS"
                    AND idx >= listingPerPage * 2 AND idx < listingPerPage * 3
                    ORDER BY RAND()
                    LIMIT 10
                ),
                ARRAY(
                    SELECT STRUCT(listing_id AS listingId, CAST(NULL AS INT64) AS rankingRank, idx AS retrievalRank, cs.source AS retrievalSrc, CAST(NULL AS INT64) AS bordaRank, CAST(NULL AS INT64) AS pageNum)
                    FROM UNNEST(candidateSources) cs,
                        UNNEST(cs.listingIds) AS listing_id WITH OFFSET idx
                    WHERE cs.stage = "POST_FILTER"
                    ORDER BY RAND()
                    LIMIT 10
                ),
                ARRAY(
                    SELECT STRUCT(listing_id AS listingId, CAST(NULL AS INT64) AS rankingRank, CAST(NULL AS INT64) AS retrievalRank, CAST(NULL AS STRING) AS retrievalSrc, idx AS bordaRank, CAST(NULL AS INT64) AS pageNum)
                    FROM UNNEST(candidateSources) cs,
                        UNNEST(cs.listingIds) AS listing_id WITH OFFSET idx
                    WHERE cs.stage = "POST_BORDA"
                    ORDER BY RAND()
                    LIMIT 10
                )
            ) listingSamples
        FROM reqsSample
        LEFT JOIN qlm USING (query, date)
        LEFT JOIN qis USING (query)
        LEFT JOIN qfb USING (query)
    ),
    flatQueryHydratedRequests AS (
        SELECT * EXCEPT (listingSamples)
        FROM queryHydratedRequests,
            UNNEST(queryHydratedRequests.listingSamples) listingSample
        WHERE guid IS NOT NULL
    ),
    outputTable AS (
        SELECT 
            flatQueryHydratedRequests.*,
            lfb.listingTitle,
            lfb.listingDescription,
            lfb.listingTaxo,
            lfb.listingTags,
        FROM flatQueryHydratedRequests
        LEFT JOIN lfb USING(listingId)
        WHERE listingTitle IS NOT NULL
    )
    SELECT
        GENERATE_UUID() AS tableUUID,
        outputTable.*
    FROM outputTable
);
    

SELECT variant_id, date, count(distinct query) 
FROM `etsy-data-warehouse-dev.search.sr-sem-rel-v1-web-results`
group by variant_id, date
order by date, variant_id




-- boe
DECLARE start_date DATE DEFAULT '2024-05-08';
DECLARE end_date DATE DEFAULT '2024-05-20';
DECLARE sample_rate FLOAT64 DEFAULT 0.01;
DECLARE listingPerPage INT64 DEFAULT 28;

create or replace table `etsy-data-warehouse-dev.search.sr-sem-rel-v1-boe-ab` as (
    with bucketing as (
        select
            bucketing_id,
            variant_id,
            bucketing_ts,
        from `etsy-data-warehouse-prod.catapult_unified.bucketing_period`
        where _date between start_date and end_date
        and experiment_id = "ranking/search.mmx.2024_q2.nrv2_sem_rel_v1_boe_try2"
    ),
    exp_visits as (
        select
            b.*,
            v.visit_id,
        from bucketing b
        join `etsy-data-warehouse-prod.weblog.visits` v
            on b.bucketing_id = v.browser_id
            and TIMESTAMP_TRUNC(bucketing_ts, SECOND) <= v.end_datetime
        where v._date between start_date and end_date
        and v.platform IN ('boe')
    ),
    beacon AS (
        select distinct
            beacon.guid,
            visit_id,
            date(_PARTITIONTIME) as date,
            (SELECT value FROM UNNEST(beacon.properties.key_value) WHERE key = 'mmx_request_uuid') AS mmxRequestUUID,
            (select value from unnest(beacon.properties.key_value) where key = 'query') as beacon_query
        from `etsy-visit-pipe-prod.canonical.visit_id_beacons`
        where date(_PARTITIONTIME) between start_date and end_date
        and beacon.event_name = 'search'
    ),
    exp_join_beacon as (
        select
            ev.variant_id,
            ev.visit_id,
            beacon.guid,
            beacon.mmxRequestUUID,
            beacon.beacon_query,
            beacon.date
        from exp_visits ev
        join beacon
        on ev.visit_id = beacon.visit_id
    )
    select distinct *
    from exp_join_beacon
);


create or replace table `etsy-data-warehouse-dev.search.sr-sem-rel-v1-boe-results` as (
    with reqsSample as (
        select 
            exp_data.*,
            r.query,
            r.etsyUUID,
            r.requestSource,
            r.candidateSources
        from `etsy-data-warehouse-dev.search.sr-sem-rel-v1-boe-ab` exp_data
        join `etsy-data-warehouse-dev.search.sr-sem-rel-v1-requests` r
        on exp_data.mmxRequestUUID = r.mmxRequestUUID
        and exp_data.date = r.date
        where r.requestSource in ('boe')
    ),
    lfb AS (
        SELECT 
            key listingId, 
            verticaListings_taxonomyPath listingTaxo,
            NULLIF(verticaListings_title, "") listingTitle,
            verticaListings_description  listingDescription,
            verticaListings_tags listingTags,
        FROM `etsy-ml-systems-prod.feature_bank_v2.listing_feature_bank_most_recent`
    ),
    qis AS (
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
        WHERE _date BETWEEN start_date and end_date
    ),
    qfb AS (
        SELECT key query, queryTaxoClassification_taxoPath queryTaxo
        FROM `etsy-ml-systems-prod.feature_bank_v2.query_feature_bank_most_recent`
    ),
    queryHydratedRequests AS (
        SELECT
            variant_id,
            etsyUUID,
            mmxRequestUUID,
            guid,
            visit_id,
            query,
            beacon_query,
            REGEXP_CONTAINS(query, '(\?i)\\bgift|\\bfor (\\bhim|\\bher|\\bmom|\\bdad|\\bmother|\\bfather|\\bdaughter|\\bson|\\bwife|\\bhusband|\\bpartner|\\baunt|\\buncle|\\bniece|\\bnephew|\\bfiance|\\bcousin|\\bin law|\\bboyfriend|\\bgirlfriend|\\bgrand|\\bfriend|\\bbest friend)') isGift,
            IFNULL(qlm.bin, 'novel') queryBin,
            IFNULL(qis.qisClass, 'missing') qisClass,
            queryTaxo,
            requestSource platform,
            reqsSample.date,
            ARRAY_CONCAT(
                ARRAY(
                    SELECT STRUCT(listing_id AS listingId, idx AS rankingRank, CAST(NULL AS INT64) AS retrievalRank, CAST(NULL AS STRING) AS retrievalSrc, CAST(NULL AS INT64) AS bordaRank, 1 AS pageNum)
                    FROM UNNEST(candidateSources) cs,
                        UNNEST(cs.listingIds) AS listing_id WITH OFFSET idx
                    WHERE cs.stage = "MO_LASTPASS"
                    AND idx < listingPerPage
                ),
                ARRAY(
                    SELECT STRUCT(listing_id AS listingId, idx AS rankingRank, CAST(NULL AS INT64) AS retrievalRank, CAST(NULL AS STRING) AS retrievalSrc, CAST(NULL AS INT64) AS bordaRank, 2 AS pageNum)
                    FROM UNNEST(candidateSources) cs,
                        UNNEST(cs.listingIds) AS listing_id WITH OFFSET idx
                    WHERE cs.stage = "MO_LASTPASS"
                    AND idx >= listingPerPage AND idx < listingPerPage * 2
                    ORDER BY RAND()
                    LIMIT 10
                ),
                ARRAY(
                    SELECT STRUCT(listing_id AS listingId, idx AS rankingRank, CAST(NULL AS INT64) AS retrievalRank, CAST(NULL AS STRING) AS retrievalSrc, CAST(NULL AS INT64) AS bordaRank, 3 AS pageNum)
                    FROM UNNEST(candidateSources) cs,
                        UNNEST(cs.listingIds) AS listing_id WITH OFFSET idx
                    WHERE cs.stage = "MO_LASTPASS"
                    AND idx >= listingPerPage * 2 AND idx < listingPerPage * 3
                    ORDER BY RAND()
                    LIMIT 10
                ),
                ARRAY(
                    SELECT STRUCT(listing_id AS listingId, CAST(NULL AS INT64) AS rankingRank, idx AS retrievalRank, cs.source AS retrievalSrc, CAST(NULL AS INT64) AS bordaRank, CAST(NULL AS INT64) AS pageNum)
                    FROM UNNEST(candidateSources) cs,
                        UNNEST(cs.listingIds) AS listing_id WITH OFFSET idx
                    WHERE cs.stage = "POST_FILTER"
                    ORDER BY RAND()
                    LIMIT 10
                ),
                ARRAY(
                    SELECT STRUCT(listing_id AS listingId, CAST(NULL AS INT64) AS rankingRank, CAST(NULL AS INT64) AS retrievalRank, CAST(NULL AS STRING) AS retrievalSrc, idx AS bordaRank, CAST(NULL AS INT64) AS pageNum)
                    FROM UNNEST(candidateSources) cs,
                        UNNEST(cs.listingIds) AS listing_id WITH OFFSET idx
                    WHERE cs.stage = "POST_BORDA"
                    ORDER BY RAND()
                    LIMIT 10
                )
            ) listingSamples
        FROM reqsSample
        LEFT JOIN qlm USING (query, date)
        LEFT JOIN qis USING (query)
        LEFT JOIN qfb USING (query)
    ),
    flatQueryHydratedRequests AS (
        SELECT * EXCEPT (listingSamples)
        FROM queryHydratedRequests,
            UNNEST(queryHydratedRequests.listingSamples) listingSample
        WHERE guid IS NOT NULL
    ),
    outputTable AS (
        SELECT 
            flatQueryHydratedRequests.*,
            lfb.listingTitle,
            lfb.listingDescription,
            lfb.listingTaxo,
            lfb.listingTags,
        FROM flatQueryHydratedRequests
        LEFT JOIN lfb USING(listingId)
        WHERE listingTitle IS NOT NULL
    )
    SELECT
        GENERATE_UUID() AS tableUUID,
        outputTable.*
    FROM outputTable
);
    

SELECT variant_id, date, count(distinct guid) 
FROM `etsy-data-warehouse-dev.search.sr-sem-rel-v1-boe-results`
group by variant_id, date
order by date, variant_id




-- after metrics are calculated by vertex pipeline
-- NDCG changes
select date, variant_id, count(distinct guid) as n_guid, avg(relevanceNDCG10) as avg_relevanceNDCG10
from `etsy-data-warehouse-dev.search.sr-sem-rel-v1-web_request-metrics` 
group by date, variant_id
order by date, variant_id

-- percent of irrelevant listings
with irr_table as (
    select date, guid, variant_id, if(classId = 1, 1.0, 0.0) as irr_listing
    from `etsy-data-warehouse-dev.search.sr-sem-rel-v1-web_query-listing-metrics_vw`
    where pageNum is not NULL
),
percent_irr as (
    select date, guid, variant_id, sum(irr_listing) / count(*) as percent_irr_listing
    from irr_table
    group by date, guid, variant_id
)
select date, variant_id, count(*) as n_guid, avg(percent_irr_listing) as avg_percent_irr
from percent_irr
group by date, variant_id
order by date, variant_id




-- by query bin & query intent
-- ndcg
with results_table as (
    select distinct guid, variant_id, query, date, queryBin, qisClass
    from `etsy-data-warehouse-dev.search.sr-sem-rel-v1-web-results`
),
ndcg_table as (
    select ndcg_res.*, queryBin, qisClass
    from `etsy-data-warehouse-dev.search.sr-sem-rel-v1-web_request-metrics` ndcg_res
    join results_table res
    on (
        ndcg_res.date = res.date
        and ndcg_res.guid = res.guid
        and ndcg_res.variant_id = res.variant_id
    )
)
select date, variant_id, queryBin, count(distinct guid) as n_guid, avg(relevanceNDCG10) as avg_relevanceNDCG10
from ndcg_table
group by queryBin, date, variant_id
order by queryBin, date, variant_id




-- sign-in / sign-out
with rpc_data as (
    SELECT
        
        request.query AS query,
        DATE(queryTime) date,
        RequestIdentifiers.etsyRequestUUID as etsyUUID,
        response.mmxRequestUUID as mmxRequestUUID,
    FROM `etsy-searchinfra-gke-prod-2.thrift_mmx_listingsv2search_search.rpc_logs_*`
    WHERE request.OPTIONS.cacheBucketId LIKE "live%"
    AND request.options.csrOrganic
    AND request.query <> ''
    AND request.offset = 0
    AND DATE(queryTime) between start_date and end_date
    AND request.options.interleavingConfig IS NULL
    AND response.count >= 88
), 



with irr_table as (
    select date, guid, variant_id, if(classId = 1, 1.0, 0.0) as irr_listing
    from `etsy-data-warehouse-dev.search.sr-sem-rel-v1-web_query-listing-metrics_vw`
    where pageNum is not NULL
),
percent_irr as (
    select date, guid, variant_id, sum(irr_listing) / count(*) as percent_irr_listing
    from irr_table
    group by date, guid, variant_id
)
select date, variant_id, count(*) as n_guid, avg(percent_irr_listing) as avg_percent_irr
from percent_irr
group by date, variant_id
order by date, variant_id



---- Segmented CVR
DECLARE start_date DATE DEFAULT '2024-05-08';
DECLARE end_date DATE DEFAULT '2024-05-20';
DECLARE my_experiment STRING DEFAULT 'ranking/search.mmx.2024_q2.nrv2_sem_rel_v1_web_try2';

create or replace table `etsy-data-warehouse-dev.search.sr-sem-rel-v1-web-ab-aggevent` as (
    with bucketing as (
        select distinct
            bucketing_id, variant_id, bucketing_ts,
        from `etsy-data-warehouse-prod.catapult_unified.bucketing_period`
        where _date = end_date
        and experiment_id = my_experiment
    ),
    -- all bucketed units during experiment
    unit_with_event as (
        SELECT distinct
            bucketing_id, variant_id, sum(event_value) as sum_event_value
        FROM `etsy-data-warehouse-prod.catapult_unified.aggregated_event_daily`
        WHERE experiment_id = my_experiment
        and _date BETWEEN start_date AND end_date
        AND event_id = "backend_cart_payment"
        group by bucketing_id, variant_id
    ),    
    --- aggregate CVR event during the experiment group by each unit
    exp_agg_events as (
        select 
            b.bucketing_id,
            b.variant_id,
            b.bucketing_ts,
            ue.sum_event_value,
        from bucketing b
        left join unit_with_event ue
        on (
            b.bucketing_id = ue.bucketing_id
            and b.variant_id = ue.variant_id
        )
    ),
    --- keep all bucketed units, add event value for units for which the event happened
    exp_visits as (
        select
            e.*,
            v.visit_id,
        from exp_agg_events e
        join `etsy-data-warehouse-prod.weblog.visits` v
            on e.bucketing_id = v.browser_id
            and TIMESTAMP_TRUNC(bucketing_ts, SECOND) <= v.end_datetime
        where v._date between start_date and end_date
    ),
    --- get visit_id for bucketed units
    beacon AS (
        select distinct
            beacon.guid,
            visit_id,
            date(_PARTITIONTIME) as date,
            (SELECT value FROM UNNEST(beacon.properties.key_value) WHERE key = 'mmx_request_uuid') AS mmxRequestUUID,
            (select value from unnest(beacon.properties.key_value) where key = 'query') as beacon_query
        from `etsy-visit-pipe-prod.canonical.visit_id_beacons`
        where date(_PARTITIONTIME) between start_date and end_date
        and beacon.event_name = 'search'
    )
    --- get beacon id and query
    select
        ev.*,
        beacon.guid,
        beacon.mmxRequestUUID,
        beacon.beacon_query,
        beacon.date
    from exp_visits ev
    join beacon
    on ev.visit_id = beacon.visit_id
);

--- append query bin and intent
create or replace table `etsy-data-warehouse-dev.search.sr-sem-rel-v1-web-ab-qseg` as (
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
        WHERE _date = end_date
    )
    select 
        ab.*, 
        qisClass, bin as queryBin
    from `etsy-data-warehouse-dev.search.sr-sem-rel-v1-web-ab-aggevent` ab
    left join qis
    on qis.query = ab.beacon_query
    left join qlm
    on qlm.query = ab.beacon_query
)


select variant_id, queryBin, count(distinct guid)
from `etsy-data-warehouse-dev.search.sr-sem-rel-v1-web-ab-qseg`
where sum_event_value is not null
group by variant_id, queryBin
order by variant_id, queryBin
