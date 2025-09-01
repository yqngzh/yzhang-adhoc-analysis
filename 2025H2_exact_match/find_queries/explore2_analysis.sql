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
