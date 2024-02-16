select count(distinct etsy_uuid)
from `etsy-search-ml-dev.ebenj.human_annotation` 
-- 1399 distinct requests

with tmp as (
    select distinct etsy_uuid, platform
    from `etsy-search-ml-dev.ebenj.human_annotation`
)
select platform, count(*) 
from tmp
group by platform
-- 1367 web requests, 32 boe requests

select count(distinct query)
from `etsy-search-ml-dev.ebenj.human_annotation` 
-- 1369 distinct queries

with tmp as (
    select distinct query, listing_id
    from `etsy-search-ml-dev.ebenj.human_annotation` 
)
select count(*) from tmp
-- 27682 distinct (query, listing) pair

with tmp as (
    select distinct etsy_uuid, query, listing_id
    from `etsy-search-ml-dev.ebenj.human_annotation` 
)
select count(*) from tmp
-- 28025 distinct (request, query, listing)

with tmp as (
    select distinct query, listing_id, platform
    from `etsy-search-ml-dev.ebenj.human_annotation` 
)
select count(*) from tmp
-- 27682

with tmp as (
    select distinct query, listing_id, platform
    from `etsy-search-ml-dev.ebenj.human_annotation` 
)
select platform, count(*)
from tmp
group by platform
-- 640 from boe
-- 27042 from web

with tmp as (
    select distinct query, bin
    from `etsy-search-ml-dev.ebenj.human_annotation` 
)
select bin, count(*)
from tmp group by bin
-- top.01 210 (15.3%)
-- top.1 203 (14.8%)
-- head 646 (47.2%)
-- torso 209 (15.3%)
-- tail 92 (6.7%)
-- novel 9 (0.6%)

with tmp as (
    select distinct query, bin, listing_id
    from `etsy-search-ml-dev.ebenj.human_annotation` 
),
n_listing_per_query as (
    select query, bin, count(*) as n_listings
    from tmp group by query, bin
)
-- select bin, avg(n_listings) as avg_n_listing, stddev(n_listings) as std_n_listings
-- from n_listing_per_query
-- group by bin
select query, n_listings
from n_listing_per_query
where bin = 'top.01'


-- query intent
with query_intent_data as (
    select query_raw, inference.label as query_intent
    from `etsy-data-warehouse-prod.arizona.query_intent_labels`
    QUALIFY ROW_NUMBER() OVER(PARTITION BY query_raw ORDER BY inference.confidence DESC) = 1
),
human_annot_data as (
    select distinct query, bin
    from `etsy-search-ml-dev.ebenj.human_annotation` 
),
merged as (
    select anno.query, anno.bin, intent.query_intent
    from human_annot_data anno
    left join query_intent_data intent
    on anno.query = intent.query_raw
)
select query_intent, count(*) as cnt
from merged
group by query_intent


with query_intent_data as (
    select 
        query_raw, 
        case 
            when class_id = 0 then 'broad'
            when class_id = 1 then 'direct-unspecified'
            when class_id = 2 then 'direct-specified'
            else null
        end as query_intent
    from `etsy-search-ml-prod.mission_understanding.qis_scores`
),
human_annot_data as (
    select distinct query, bin
    from `etsy-search-ml-dev.ebenj.human_annotation` 
),
merged as (
    select anno.query, anno.bin, intent.query_intent
    from human_annot_data anno
    left join query_intent_data intent
    on anno.query = intent.query_raw
)
select query_intent, count(*) as cnt
from merged
group by query_intent


-- query listing taxonomy mismatch
with human_annot_data as (
    select distinct query, bin, listing_id, taxonomy as query_taxo
    from `etsy-search-ml-dev.ebenj.human_annotation` 
),
listing_taxo_data as (
    select alb.listing_id, taxo.full_path as listing_taxo
    from `etsy-data-warehouse-prod.rollups.active_listing_basics` alb
    join `etsy-data-warehouse-prod.structured_data.taxonomy` taxo
    on alb.taxonomy_id = taxo.taxonomy_id
),
query_intent_data as (
    select query_raw, inference.label as query_intent
    from `etsy-data-warehouse-prod.arizona.query_intent_labels`
    QUALIFY ROW_NUMBER() OVER(PARTITION BY query_raw ORDER BY inference.confidence DESC) = 1
),
merged as (
    select human_annot_data.*, listing_taxo, query_intent
    from human_annot_data 
    left join listing_taxo_data
    on human_annot_data.listing_id = listing_taxo_data.listing_id
    left join query_intent_data
    on human_annot_data.query = query_intent_data.query_raw
)
select query_intent, count(*)
from merged
where query_taxo is not null and listing_taxo is not null
and query_taxo != listing_taxo
group by query_intent
order by query_intent


with human_annot_data as (
    select distinct query, bin, listing_id, taxonomy as query_taxo
    from `etsy-search-ml-dev.ebenj.human_annotation` 
),
listing_taxo_data as (
    select alb.listing_id, taxo.full_path as listing_taxo
    from `etsy-data-warehouse-prod.rollups.active_listing_basics` alb
    join `etsy-data-warehouse-prod.structured_data.taxonomy` taxo
    on alb.taxonomy_id = taxo.taxonomy_id
),
query_intent_data as (
    select 
        query_raw, 
        case 
            when class_id = 0 then 'broad'
            when class_id = 1 then 'direct-unspecified'
            when class_id = 2 then 'direct-specified'
            else null
        end as query_intent
    from `etsy-search-ml-prod.mission_understanding.qis_scores`
),
merged as (
    select human_annot_data.*, listing_taxo, query_intent
    from human_annot_data 
    left join listing_taxo_data
    on human_annot_data.listing_id = listing_taxo_data.listing_id
    left join query_intent_data
    on human_annot_data.query = query_intent_data.query_raw
)
select query_intent, count(*)
from merged
where query_taxo is not null and listing_taxo is not null
and query_taxo != listing_taxo
group by query_intent
order by query_intent
-- both query and listing taxo available: 20983 pairs
-- query taxo != listing taxo: 17013 / 20983 (81%)
-- evenly distributed across intent groups


-- qualitative
create or replace table `etsy-sr-etl-prod.yzhang.semantic-relevance-human-anno-data1` as (
    with human_annot_data as (
        select query, bin, listing_id, title, description, taxonomy as query_taxo, label
        from `etsy-search-ml-dev.ebenj.human_annotation` 
    ),
    listing_taxo_data as (
        select alb.listing_id, taxo.full_path as listing_taxo
        from `etsy-data-warehouse-prod.rollups.active_listing_basics` alb
        join `etsy-data-warehouse-prod.structured_data.taxonomy` taxo
        on alb.taxonomy_id = taxo.taxonomy_id
    ),
    query_intent_data as (
        select 
            query_raw, 
            case 
                when class_id = 0 then 'broad'
                when class_id = 1 then 'direct-unspecified'
                when class_id = 2 then 'direct-specified'
                else null
            end as query_intent
        from `etsy-search-ml-prod.mission_understanding.qis_scores`
    )
    select human_annot_data.*, listing_taxo, query_intent
    from human_annot_data 
    left join listing_taxo_data
    on human_annot_data.listing_id = listing_taxo_data.listing_id
    left join query_intent_data
    on human_annot_data.query = query_intent_data.query_raw
)


-- get annotator agreement from raw data
SELECT
 cl.radio_answer.value as type,
 COUNT(*) as count
FROM
 `etsy-search-ml-dev.adhoc.human_annotation_comparison_results_raw`,
 UNNEST(projects.clpiv9jwj0791071a6iha7gx7.labels) AS lb,
 UNNEST(lb.annotations.classifications) as cl
GROUP BY
 type
ORDER BY
 count DESC;