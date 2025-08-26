declare start_date date default "2025-07-25";
declare end_date date default "2025-08-25";

create or replace table `etsy-search-ml-dev.search.yzhang_emqueries_step1` as (
    with allrpc as (
        SELECT
            response.mmxRequestUUID as mmxRequestUUID,
            COALESCE((SELECT NULLIF(query, '') FROM UNNEST(request.filter.query.translations) WHERE language = 'en'), NULLIF(request.query, '')) query,
            DATE(queryTime) as date,
            IF(request.options.personalizationOptions.userId > 0, "SI", "SO") si_so
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
    semrel_teacher_page1 as (
        SELECT
            mmxRequestUUID,
            guid,
            query,
            listingId,
            r.date,
            platform,
            userLanguage,
            userCountry,
            CASE 
                WHEN classId = 1 THEN 'Irrelevant' 
                WHEN classId = 2 THEN 'Partial'
                WHEN classId = 3 THEN 'Relevant' 
            END AS semrelClass,
            softmaxScores,
            rankingRank
        FROM `etsy-data-warehouse-prod.search.sem_rel_hydrated_daily_requests` r
        JOIN `etsy-data-warehouse-prod.search.sem_rel_query_listing_metrics` USING (tableUUID)
        WHERE modelName = "v3-finetuned-llama-8b" 
        AND r.date between start_date and end_date
        AND pageNum = 1
        AND r.resultType = "organic"
    ),
    selected_data as (
        select sr.*
        from semrel_teacher_page1 sr
        left join allrpc using (mmxRequestUUID, query, date)
        where si_so = "SO"
        and userLanguage = "en-US"
        and userCountry = "US"
    ),
    agg_requests as (
        select 
            mmxRequestUUID, 
            guid,
            query, 
            date,
            platform, 
            count(*) as n_total, 
            sum(if(semrelClass = 'Relevant', 1, 0)) as n_em,
            array_agg(listingId order by rankingRank asc) as listingIdArray,
            array_agg(semrelClass order by rankingRank asc) as semrelClassArray,
            array_agg(rankingRank order by rankingRank asc) as rankingRankArray,
        from selected_data
        group by mmxRequestUUID, guid, date, query, platform
    ),
    valid_requests as (
        select * from agg_requests
        where (
            (platform = "web" and n_total = 48) or
            (platform = "mweb" and n_total = 34) or 
            (platform = "boe" and n_total = 28)
        )
    ),
    valid_requests_w_pct_em as (
        select *, n_em / n_total as pct_em
        from valid_requests
    ),
    qlm AS (
      select distinct query_raw as query, bin as queryBin 
      from `etsy-data-warehouse-prod.rollups.query_level_metrics_raw`
    )
    select req.*, queryBin 
    from valid_requests_w_pct_em req
    left join qlm using (query)
    where pct_em <= 0.6
)
