-- all single segments
-- query bin
-- qisClass
-- queryTaxo
-- QEE
-- with and without prior engagements
-- result count

-- one day of valid rpc logs, distribution of query
WITH allRequests AS (
    SELECT
        (SELECT COUNTIF(stage='MO_LASTPASS') FROM UNNEST(OrganicRequestMetadata.candidateSources)) nLastpassSources,
        (SELECT COUNTIF(stage='POST_FILTER') FROM UNNEST(OrganicRequestMetadata.candidateSources)) nPostFilterSources,
        (SELECT COUNTIF(stage='POST_BORDA') FROM UNNEST(OrganicRequestMetadata.candidateSources)) nPostBordaSources,
        (SELECT COUNTIF(stage='POST_SEM_REL_FILTER') FROM UNNEST(OrganicRequestMetadata.candidateSources)) nPostSemrelSources,
        COALESCE((SELECT NULLIF(query, '') FROM UNNEST(request.filter.query.translations) WHERE language = 'en'), NULLIF(request.query, '')) query,
        request.options.userLanguage userLanguage,
        request.options.userCountry userCountry,
        response.mmxRequestUUID as mmxRequestUUID,
        IF(request.options.personalizationOptions.userId > 0, "SI", "SO") si_so
    FROM `etsy-searchinfra-gke-prod-2.thrift_mmx_listingsv2search_search.rpc_logs_*`
    WHERE request.OPTIONS.cacheBucketId LIKE "live%"
    AND request.options.csrOrganic
    AND request.options.searchPlacement IN ('wsg', 'allsr', 'wmg')
    AND request.query <> ''
    AND request.offset = 0
    AND DATE(queryTime) = "2025-08-24"
    AND request.options.interleavingConfig IS NULL
    AND NOT EXISTS (
      SELECT * FROM UNNEST(request.context)
      WHERE key = "req_source" AND value = "bot"
    )
),
validRequests AS (
    SELECT distinct *
    FROM allRequests
    WHERE nLastPassSources=1
    AND nPostFilterSources>=1
    AND (nPostBordaSources>=1 OR nPostSemrelSources>=1)
    AND userLanguage = "en-US"
    AND userCountry = "US"
    AND si_so = "SO"
),
qlm as (
    select distinct query_raw as query, bin as queryBin 
    from `etsy-data-warehouse-prod.rollups.query_level_metrics_raw`
),
qisv3 AS (
    select distinct query_raw query,
    CASE 
    WHEN prediction = 0 THEN 'broad' 
    WHEN prediction = 1 THEN 'direct_unspecified'
    WHEN prediction = 2 THEN 'direct_specified' 
    END as qisClass    
    from `etsy-search-ml-prod.mission_understanding.qis_scores_v3`
),
qtcv5 AS (
    SELECT DISTINCT
        COALESCE(s.query, b.query) AS query,
        COALESCE(s.full_path, b.full_path) AS queryTaxoFullPath
    FROM `etsy-data-warehouse-prod.mission_understanding.query_taxonomy_seller` s
    FULL OUTER JOIN `etsy-data-warehouse-prod.mission_understanding.query_taxonomy_buyer` b
    USING(query)
),
qee AS (
    SELECT
        searchQuery AS query,
        ANY_VALUE(fandom)        AS queryEntities_fandom,
        ANY_VALUE(motif)         AS queryEntities_motif,
        ANY_VALUE(style)         AS queryEntities_style,
        ANY_VALUE(material)      AS queryEntities_material,
        ANY_VALUE(color)         AS queryEntities_color,
        ANY_VALUE(technique)     AS queryEntities_technique,
        ANY_VALUE(tangibleItem)  AS queryEntities_tangibleItem,
        ANY_VALUE(size)          AS queryEntities_size,
        ANY_VALUE(occasion)      AS queryEntities_occasion,
        ANY_VALUE(customization) AS queryEntities_customization,
        ANY_VALUE(age)           AS queryEntities_age,
        ANY_VALUE(price)         AS queryEntities_price,
        ANY_VALUE(quantity)      AS queryEntities_quantity,
        ANY_VALUE(recipient)     AS queryEntities_recipient
    FROM `etsy-search-ml-prod.mission_understanding.query_entity_features`
    GROUP BY query
),
merged as (
    select distinct
        mmxRequestUUID, query, queryBin, qisClass, 
        SPLIT(queryTaxoFullPath, ".")[OFFSET(0)] AS queryTaxoTop,
        queryEntities_fandom,
        queryEntities_motif,
        queryEntities_style,
        queryEntities_material,
        queryEntities_color,
        queryEntities_technique,
        queryEntities_tangibleItem,
        queryEntities_size,
        queryEntities_occasion,
        queryEntities_customization,
        queryEntities_age,
        queryEntities_price,
        queryEntities_quantity,
        queryEntities_recipient
    from validRequests
    left join qlm using (query)
    left join qisv3 using (query)
    left join qtcv5 using (query)
    left join qee using (query)
)
select queryBin, count(*)
from merged 
group by queryBin


with tmp as (
    select distinct mmxRequestUUID, query, queryBin
    from `etsy-search-ml-dev.search.yzhang_emqueries_aug_result`
)
select queryBin, count(*)
from tmp
group by queryBin


--- chatgpt list
-- definitely attribute: material, color, size, price, quantity
-- fuzzy/semi attributes, less universally structured: occasion, age, recipient, style, motif, fandom, technique, customization