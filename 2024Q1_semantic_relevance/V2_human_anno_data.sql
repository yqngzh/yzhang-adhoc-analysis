-- 29990 distinct query, listing pairs

select distinct query, listingId
from `etsy-search-ml-dev.human_annotation.survey2_examples_w_labels`
-- 3 labels
-- relevant: 16622
-- partial: 9358
-- not relevant: 3976
-- not_sure_q1: 16
-- not_sure_broad: 18

select count(distinct query)
from `etsy-search-ml-dev.human_annotation.survey2_examples_w_labels`
where relevance_value not like 'not_sure%'
-- 29956 pairs with a label judgement
-- 21706 distinct queries

select query, listingTitle, listingId, relevance_value
from `etsy-search-ml-dev.human_annotation.survey2_examples_w_labels`
where relevance_value like 'not_sure%'
order by relevance_value

with tmp as (
    select distinct query, isGift
    from `etsy-search-ml-dev.human_annotation.survey2_examples_w_labels`
    where relevance_value not like 'not_sure%'
)
select isGift, count(*)
from tmp
group by isGift
-- 1704 (7.9%) gift query out of 21706 is gift query

-- direct_specified 8631	
-- direct_unspecified 7835	
-- broad 5193
-- missing 47

-- top.01 5663
-- top.1 4421
-- head 6215
-- torso 1621
-- tail 653
-- novel 3141

select relevance_value, count(*)
from `etsy-search-ml-dev.human_annotation.survey2_examples_w_labels`
where relevance_value like 'not_sure%'
group by relevance_value


-- queries mapped to multiple bins
with tmp as (
    select distinct query, bin
    from `etsy-search-ml-dev.human_annotation.survey2_examples_w_labels`
    where relevance_value not like 'not_sure%'
)
select *
from tmp
where query in (
    'minimalist prints',
    'Womens spring dresses',
    'denim decor',
    'vintage style',
    'Valentines gifts',
    'hazbin hotel alastor',
    'coaches gifts basketball'
)

-- sample 100 random  pairs
with tmp as (
    select query, bin, listingId, listingTitle, relevance_value
    from `etsy-search-ml-dev.human_annotation.survey2_examples_w_labels`
    where relevance_value not like 'not_sure%'
    and rand() < 0.1
)
select * from tmp
limit 100
