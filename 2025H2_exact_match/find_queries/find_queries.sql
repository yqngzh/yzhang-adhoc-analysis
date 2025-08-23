create or replace table `etsy-search-ml-dev.search.yzhang_emqueries_try1` as (
    with semrel_teacher_results as (
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
        AND r.date between "2025-07-22" and "2025-08-22"
        AND pageNum = 1
        AND r.resultType = "organic"
    ),
    agg_requests as (
        select 
            mmxRequestUUID, 
            guid,
            query, 
            date,
            platform, 
            userLanguage,
            userCountry,
            count(*) as n_total, 
            sum(if(semrelClass = 'Relevant', 1, 0)) as n_em,
            array_agg(listingId order by rankingRank asc) as listingIdArray,
            array_agg(semrelClass order by rankingRank asc) as semrelClassArray,
            array_agg(rankingRank order by rankingRank asc) as rankingRankArray,
        from semrel_teacher_results
        group by mmxRequestUUID, guid, date, query, platform, userLanguage, userCountry
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
    where queryBin in ("top.01", "top.1", "head", "tail")
    and pct_em <= 0.6
)

select distinct query, date, queryBin, pct_em
from `etsy-search-ml-dev.search.yzhang_emqueries_try1`
where platform = "web"
and userLanguage = "en-US"
and userCountry = "US"
and date = "2025-08-22"
order by queryBin desc