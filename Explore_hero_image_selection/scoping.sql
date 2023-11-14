-- total n listing 133,186,415
-- sum(past_year_gms) 8541983604.97
-- 5% listings 6659320, round up 7000000 listings
-- GMS 7486221779.43 (87%)

-- pick listings with descending past year gms
create or replace table `etsy-sr-etl-prod.yzhang.rollup_listing_top_gms_2023-11-13` as (
    select listing_id, past_year_gms
    from `etsy-data-warehouse-prod.rollups.active_listing_basics`
    order by past_year_gms desc
    limit 7000000
)

