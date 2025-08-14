-- create base data
create or replace table `etsy-search-ml-dev.yzhang.exact_matches_base_data_v4_1` as (
    with excel_data as (
        SELECT
            mmxRequestUUID, 
            query, 
            listing_id, 
            Relevance_Class AS relevance_class,
            Product_type_in_query AS product_type_in_query,
            Product_type_in_listing_if_mismatch AS product_type_in_listing_if_mismatch,
            Subsitute___Complementary AS subsitute_complementary,
            Descriptor_Mismatch AS descriptor_mismatch
        FROM `etsy-data-warehouse-dev.dblincoe.exact_matches_annotation_july_2025`
    ),
    source_from_semrel_table as (
        SELECT DISTINCT 
            r.etsyUUID,
            r.platform,
            r.userCountry,
            r.userLanguage,
            r.mmxRequestUUID,
            r.query AS query,
            REGEXP_CONTAINS(query, '(\?i)\\bgift|\\bfor (\\bhim|\\bher|\\bmom|\\bdad|\\bmother|\\bfather|\\bdaughter|\\bson|\\bwife|\\bhusband|\\bpartner|\\baunt|\\buncle|\\bniece|\\bnephew|\\bfiance|\\bcousin|\\bin law|\\bboyfriend|\\bgirlfriend|\\bgrand|\\bfriend|\\bbest friend)') queryIsGift,
            r.listingId,
            "partial" as v3_teacher_label
        FROM  `etsy-data-warehouse-prod.search.sem_rel_hydrated_daily_requests` r
        JOIN `etsy-data-warehouse-prod.search.sem_rel_query_listing_metrics` m USING (tableUUID, date)
        WHERE r.date BETWEEN '2025-06-01' AND '2025-06-30'
        AND m.modelName = 'v3-finetuned-llama-8b'
        AND m.classId = 2
        AND r.pageNum IN (1, 2, 3)
        AND r.userLanguage = 'en-US'
    ),
    merged as (
        select distinct
            etsyUUID, platform, userCountry, userLanguage,
            excel_data.query, 
            "" as queryEn,
            listingid as listingId,
            relevance_class as gt_label,
            product_type_in_query, 
            product_type_in_listing_if_mismatch,
            subsitute_complementary,
            descriptor_mismatch,
            queryIsGift,
            v3_teacher_label,
            "partial_purchases" as anno_data_source,
        from excel_data
        left join source_from_semrel_table on (
            excel_data.query = source_from_semrel_table.query and
            excel_data.listing_id = source_from_semrel_table.listingId
        )
    ),
    ql_count as (
        select query, listingId, count(*) as cnt 
        from merged
        group by query, listingId
    ),
    deduplicated as (
        select merged.*
        from merged
        join ql_count using (query, listingId)
        where cnt = 1
    )
    select
        GENERATE_UUID() AS row_id,
        *
    from deduplicated
)
-- inner join 424; could not join on mmxRequestUUID because only 3 left pairs


-- hydrate features same methods as V3
create or replace table `etsy-search-ml-dev.yzhang.exact_matches_base_data_v4_1` as (

)