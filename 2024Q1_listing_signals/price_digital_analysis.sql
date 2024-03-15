------  Digital analysis (web)
select count(*)
from `etsy-sr-etl-prod.yzhang.lsig_v5_web_tight_0305`
where attributions = 1
and listing_is_digital is not null and query_is_digital is not null
and listing_is_digital != query_is_digital
-- 4365056 rows
-- 103870 distinct requests  (108701 in data, 95%)
-- 105606 rows has purchase  (110498 in data, 95%)
-- 40010 (38%) digital listings, 65596 (62%) non-digital listings

-- 72306 purchases have both listing and query digital status
-- 53514 query listing match
-- 7499 purchases have both query & listing digital
-- 18792 (26%) made when query and listing digital not match 
-- 18755 query not digial, listing digital


-- Purchases from digital queries
select count(*)
from `etsy-sr-etl-prod.yzhang.lsig_v5_web_tight_0305`
where attributions = 1
and query_is_digital = 1
-- 7536 (10%) purchases from digital queries
-- 7499 listing digital

-- Purchases from non-digital queries
select count(*)
from `etsy-sr-etl-prod.yzhang.lsig_v5_web_tight_0305`
where attributions = 1
and query_is_digital = 0
-- 64770 purchases from non-digital queries
-- 18755 (29%) listing digital


select avg(lw_price)
from `etsy-sr-etl-prod.yzhang.lsig_v5_web_tight_0305`
where attributions = 1
and listing_is_digital = 1
-- digital listing avg price: 6.3
-- not average digital listing: 28


-- if query is digital, unlikely to buy not digital listings
---- if query is not digital, 30% purchases digital listings
-- 7% requests have digital queries, account for 10% purchases
---- 93% requests have non-digital queries, account for 90% purchases
-- digital listings account for 40% purchases, with significantly lower price

-- observations on 02/28 are the same


------  Price analysis
-- 8921 / 4365056 price usd missing
--- in notebook



------  Digital analysis (BoE)
select count(*)
from `etsy-sr-etl-prod.yzhang.lsig_v5_boe_tight_0312`
where attributions = 1
and listing_is_digital is not null and query_is_digital is not null
and listing_is_digital != query_is_digital
-- 2,492,024 rows
-- 88093 distinct requests  
-- 91758 rows has purchase  
-- 20756 (22%) digital listings, 71002 (77%) non-digital listings

-- 69940 purchases have both listing and query digital status
-- 57651 query listing match
-- 2877 purchases have both query & listing digital
-- 12289 (17%) made when query and listing digital not match 
-- 12234 query not digial, listing digital

-- Purchases from digital queries
select count(*)
from `etsy-sr-etl-prod.yzhang.lsig_v5_boe_tight_0312`
where attributions = 1
and query_is_digital = 1
-- 2932 (3%) purchases from digital queries
-- 2877 listing digital

-- Purchases from non-digital queries
select count(*)
from `etsy-sr-etl-prod.yzhang.lsig_v5_boe_tight_0312`
where attributions = 1
and query_is_digital = 0
-- 67008 purchases from non-digital queries
-- 12234 (18%) listing digital


with tmp as (
    select distinct requestUUID, query_is_digital
    from `etsy-sr-etl-prod.yzhang.lsig_v5_boe_tight_0312`
)
select count(*) from tmp
where query_is_digital = 1
-- 2781 (3%) requests have digital query


select avg(lw_price)
from `etsy-sr-etl-prod.yzhang.lsig_v5_boe_tight_0312`
where attributions = 1
and listing_is_digital = 1
-- digital listing avg price: 7.6
-- not average digital listing: 25.6


-- if query is digital, unlikely to buy not digital listings
---- if query is not digital, 18% purchases digital listings
-- 2.5% requests have digital queries, account for 3% purchases
-- digital listings account for 22% purchases, with significantly lower price

-- similar stats 0226
