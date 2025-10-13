----  overall distribution
with tmp as (
    select distinct queryBin, mmxRequestUUID
    from `etsy-search-ml-dev.search.yzhang_emqueries_issue_llm_clean`
)
select queryBin, count(*) 
from tmp
group by queryBin
order by queryBin
-- bin, qis, queryTaxo

select count(distinct mmxRequestUUID) 
from `etsy-search-ml-dev.search.yzhang_emqueries_issue_llm_clean`
where array_length(queryEntities_tangibleItem) > 0

with qee_count as (
    select
        *,
        array_length(queryEntities_material) +
            array_length(queryEntities_color) +
            array_length(queryEntities_size) +
            array_length(queryEntities_occasion) +
            array_length(queryEntities_age) +
            array_length(queryEntities_price) +
            array_length(queryEntities_quantity) +
            array_length(queryEntities_recipient) +
            array_length(queryEntities_fandom) +
            array_length(queryEntities_motif) +
            array_length(queryEntities_style) +
            array_length(queryEntities_technique) +
            array_length(queryEntities_customization) as qee_attribute_count,
        array_length(queryEntities_material) +
            array_length(queryEntities_color) +
            array_length(queryEntities_size) +
            array_length(queryEntities_price) +
            array_length(queryEntities_quantity) as qee_hard_attribute_count
    from `etsy-search-ml-dev.search.yzhang_emqueries_issue_llm_clean`
)
select count(distinct mmxRequestUUID)
from qee_count
where qee_attribute_count >= 1
and array_length(queryEntities_tangibleItem) > 0




----  identified problematic pages
CREATE OR REPLACE TABLE `etsy-search-ml-dev.search.yzhang_emqueries_issue_problem_requests` AS (
    with agg_requests as (
        select 
            mmxRequestUUID,
            count(*) as n_total,
            sum(if(llm_final_label = "relevant", 1, 0)) as n_relevant,
            sum(if(llm_final_label != "relevant" and listingRank < 12, 1, 0)) as n_top_irr,
            sum(if(llm_final_label = "relevant" and listingRank >= 12, 1, 0)) as n_bottom_rel,
        from `etsy-search-ml-dev.search.yzhang_emqueries_issue_llm_clean`
        group by mmxRequestUUID
    ),
    problematic_requests as (
        select mmxRequestUUID
        from agg_requests
        where n_relevant / n_total <= 0.6
        and (
            n_top_irr >= 3 and n_bottom_rel >= 3
        )
    )
    select *
    from `etsy-search-ml-dev.search.yzhang_emqueries_issue_llm_clean`
    where mmxRequestUUID in (
        select mmxRequestUUID from problematic_requests
    )
)


with tmp as (
    select distinct queryBin, mmxRequestUUID
    from `etsy-search-ml-dev.search.yzhang_emqueries_issue_problem_requests`
)
select queryBin, count(*) 
from tmp
group by queryBin

select count(distinct mmxRequestUUID) 
from `etsy-search-ml-dev.search.yzhang_emqueries_issue_problem_requests`
where array_length(queryEntities_tangibleItem) > 0

with qee_count as (
    select
        *,
        array_length(queryEntities_material) +
            array_length(queryEntities_color) +
            array_length(queryEntities_size) +
            array_length(queryEntities_occasion) +
            array_length(queryEntities_age) +
            array_length(queryEntities_price) +
            array_length(queryEntities_quantity) +
            array_length(queryEntities_recipient) +
            array_length(queryEntities_fandom) +
            array_length(queryEntities_motif) +
            array_length(queryEntities_style) +
            array_length(queryEntities_technique) +
            array_length(queryEntities_customization) as qee_attribute_count,
        array_length(queryEntities_material) +
            array_length(queryEntities_color) +
            array_length(queryEntities_size) +
            array_length(queryEntities_price) +
            array_length(queryEntities_quantity) as qee_hard_attribute_count
    from `etsy-search-ml-dev.search.yzhang_emqueries_issue_problem_requests`
)
select count(distinct mmxRequestUUID)
from qee_count
where qee_attribute_count >= 1
and array_length(queryEntities_tangibleItem) > 0



-- ============================================================
-- Expand QEE
-- ============================================================
create or replace table `etsy-search-ml-dev.search.yzhang_emqueries_issue_llm_clean_wqee` as (
    with tmp as (
        select 
            a.*,
            b.queryEntities as new_queryEntities,
            b.queryEntities_fandom as new_queryEntities_fandom,
            b.queryEntities_motif as new_queryEntities_motif,
            b.queryEntities_style as new_queryEntities_style,
            b.queryEntities_material as new_queryEntities_material,
            b.queryEntities_color as new_queryEntities_color,
            b.queryEntities_technique as new_queryEntities_technique,
            b.queryEntities_tangibleItem as new_queryEntities_tangibleItem,
            b.queryEntities_size as new_queryEntities_size,
            b.queryEntities_occasion as new_queryEntities_occasion,
            b.queryEntities_customization as new_queryEntities_customization,
            b.queryEntities_age as new_queryEntities_age,
            b.queryEntities_price as new_queryEntities_price,
            b.queryEntities_quantity as new_queryEntities_quantity,
            b.queryEntities_recipient as new_queryEntities_recipient
        from `etsy-search-ml-dev.search.yzhang_emqueries_issue_llm_clean` a
        left join `etsy-search-ml-dev.search.yzhang_emqueries_issue_expand_qee` b
        using (query, queryEn)
    )
    select 
        mmxRequestUUID,
        query, 
        queryEn,
        queryDate,
        platform,
        userLanguage,
        userCountry,
        si_so,
        listingId,
        listingRank,
        queryBin,
        qisClass,
        queryRewrites,
        queryTaxoFullPath,
        queryTaxoTop,
        if(tmp.queryEntities is null or tmp.queryEntities = '', tmp.new_queryEntities, tmp.queryEntities) queryEntities,
        if(tmp.queryEntities is null or tmp.queryEntities = '', tmp.new_queryEntities_fandom, tmp.queryEntities_fandom) queryEntities_fandom,
        if(tmp.queryEntities is null or tmp.queryEntities = '', tmp.new_queryEntities_motif, tmp.queryEntities_motif) queryEntities_motif,
        if(tmp.queryEntities is null or tmp.queryEntities = '', tmp.new_queryEntities_style, tmp.queryEntities_style) queryEntities_style,
        if(tmp.queryEntities is null or tmp.queryEntities = '', tmp.new_queryEntities_material, tmp.queryEntities_material) queryEntities_material,
        if(tmp.queryEntities is null or tmp.queryEntities = '', tmp.new_queryEntities_color, tmp.queryEntities_color) queryEntities_color,
        if(tmp.queryEntities is null or tmp.queryEntities = '', tmp.new_queryEntities_technique, tmp.queryEntities_technique) queryEntities_technique,
        if(tmp.queryEntities is null or tmp.queryEntities = '', tmp.new_queryEntities_tangibleItem, tmp.queryEntities_tangibleItem) queryEntities_tangibleItem,    
        if(tmp.queryEntities is null or tmp.queryEntities = '', tmp.new_queryEntities_size, tmp.queryEntities_size) queryEntities_size,    
        if(tmp.queryEntities is null or tmp.queryEntities = '', tmp.new_queryEntities_occasion, tmp.queryEntities_occasion) queryEntities_occasion,      
        if(tmp.queryEntities is null or tmp.queryEntities = '', tmp.new_queryEntities_customization, tmp.queryEntities_customization) queryEntities_customization,    
        if(tmp.queryEntities is null or tmp.queryEntities = '', tmp.new_queryEntities_age, tmp.queryEntities_age) queryEntities_age,    
        if(tmp.queryEntities is null or tmp.queryEntities = '', tmp.new_queryEntities_price, tmp.queryEntities_price) queryEntities_price,  
        if(tmp.queryEntities is null or tmp.queryEntities = '', tmp.new_queryEntities_quantity, tmp.queryEntities_quantity) queryEntities_quantity,  
        if(tmp.queryEntities is null or tmp.queryEntities = '', tmp.new_queryEntities_recipient, tmp.queryEntities_recipient) queryEntities_recipient, 
        listingCountry,
        shop_primaryLanguage,
        listingTitle,
        listingTitleEn,
        listingTaxo,
        listingTags,
        listingAttributes,
        listingShopName,
        listingDescription,
        listingDescriptionEn,
        listingDescNgrams,
        listingImageUrls,
        listingHeroImageCaption,
        listingVariations,
        listingReviews,
        listingEntities_tangibleItem,
        listingEntities_material,
        listingEntities_color,
        listingEntities_style,
        listingEntities_size,
        listingEntities_occasion,
        listingEntities_customization,
        listingEntities_technique,
        listingEntities_fandom,
        listingEntities_brand,
        listingEntities_quantity,
        listingEntities_recipient,
        listingEntities_age,
        listingEntities_misc,
        llm_final_label,
        llm_consensus_type
    from tmp
)


CREATE OR REPLACE TABLE `etsy-search-ml-dev.search.yzhang_emqueries_issue_problem_requests_wqee` AS (
    with agg_requests as (
        select 
            mmxRequestUUID,
            count(*) as n_total,
            sum(if(llm_final_label = "relevant", 1, 0)) as n_relevant,
            sum(if(llm_final_label != "relevant" and listingRank < 12, 1, 0)) as n_top_irr,
            sum(if(llm_final_label = "relevant" and listingRank >= 12, 1, 0)) as n_bottom_rel,
        from `etsy-search-ml-dev.search.yzhang_emqueries_issue_llm_clean_wqee`
        group by mmxRequestUUID
    ),
    problematic_requests as (
        select mmxRequestUUID
        from agg_requests
        where n_relevant / n_total <= 0.6
        and (
            n_top_irr >= 3 and n_bottom_rel >= 3
        )
    )
    select *
    from `etsy-search-ml-dev.search.yzhang_emqueries_issue_llm_clean_wqee`
    where mmxRequestUUID in (
        select mmxRequestUUID from problematic_requests
    )
)



-- ============================================================
-- Queries with or without prior engagement
-- ============================================================
create or replace table `etsy-search-ml-dev.search.yzhang_emqueries_issue_llm_clean_full` as (
    with qlm as (
        select distinct 
            query_raw as query, 
            if(total_clicks > 0, "has_clicks", "no_clicks") as queryPriorClicks,
            if(total_purchases > 0, "has_purchase", "no_purchase") as queryPriorPurchase,
        from `etsy-data-warehouse-prod.rollups.query_level_metrics_raw`
    )
    select 
        x.*,
        queryPriorClicks, queryPriorPurchase
    from `etsy-search-ml-dev.search.yzhang_emqueries_issue_llm_clean_wqee` x
    left join qlm using (query)
)

create or replace table `etsy-search-ml-dev.search.yzhang_emqueries_issue_problem_requests_full` as (
    with qlm as (
        select distinct 
            query_raw as query, 
            REGEXP_CONTAINS(query, '(\?i)\\bgift|\\bfor (\\bhim|\\bher|\\bmom|\\bdad|\\bmother|\\bfather|\\bdaughter|\\bson|\\bwife|\\bhusband|\\bpartner|\\baunt|\\buncle|\\bniece|\\bnephew|\\bfiance|\\bcousin|\\bin law|\\bboyfriend|\\bgirlfriend|\\bgrand|\\bfriend|\\bbest friend)') queryIsGift,
            if(total_clicks > 0, "has_clicks", "no_clicks") as queryPriorClicks,
            if(total_purchases > 0, "has_purchase", "no_purchase") as queryPriorPurchase,
        from `etsy-data-warehouse-prod.rollups.query_level_metrics_raw`
    )
    select 
        x.*,
        queryIsGift, queryPriorClicks, queryPriorPurchase
    from `etsy-search-ml-dev.search.yzhang_emqueries_issue_problem_requests_wqee` x
    left join qlm using (query)
)

with tmp as (
    select distinct queryPriorClicks, mmxRequestUUID
    from `etsy-search-ml-dev.search.yzhang_emqueries_issue_llm_clean_full`
)
select queryPriorClicks, count(*) 
from tmp
group by queryPriorClicks
order by queryPriorClicks


create or replace table `etsy-search-ml-dev.search.yzhang_emqueries_issue_query_engagement` as (
    -- has any purchase or clicks in the past year
    select distinct 
        query_raw as query, 
        if(total_clicks > 0, "has_clicks", "no_clicks") as queryPriorClicks,
        if(total_purchases > 0, "has_purchase", "no_purchase") as queryPriorPurchase,
    from `etsy-data-warehouse-prod.rollups.query_level_metrics_raw`
)