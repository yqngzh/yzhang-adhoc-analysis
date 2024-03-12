-- total in feature bank, 969,090,857 listings

---- Tags
select count(*)
from `etsy-ml-systems-prod.feature_bank_v2.listing_feature_bank_2024-02-08`
where EDIT_DISTANCE(lower(listingWeb_tags), lower(verticaListings_tags)) > 1
-- 169301284 both field not missing
-- all 169301284 lower case not equal
-- 23209572 (14%) lower case tags edit distance > 1

select count(*)
from `etsy-ml-systems-prod.feature_bank_v2.listing_feature_bank_2024-02-08`
where lower(listingWeb_tags) is not null
and lower(verticaListings_tags) is null
-- 305510 listings listing Web has value, vertica listing doesn't
-- reverse 791669902 listings

-- 9293098 listingWeb has more tags


---- Price
select count(*)
from `etsy-ml-systems-prod.feature_bank_v2.listing_feature_bank_2024-02-08`
where listingWeb_price is not null and activeListingBasics_priceUsd is not null
-- 158063227 has both fields

with us_price_data as (
    select 
        cast(listingWeb_price.key_value[array_length(listingWeb_price.key_value)-1].value as float64) / 100.0 as listing_web_price, 
        activeListingBasics_priceUsd
    from `etsy-ml-systems-prod.feature_bank_v2.listing_feature_bank_2024-02-08`
    where listingWeb_price is not null and activeListingBasics_priceUsd is not null
)
select count(*) from us_price_data
where abs(listing_web_price - activeListingBasics_priceUsd) > 1.0
-- 47227077 (30%) price different
-- 23149632 (14.6%) price diff > 1

select count(*)
from `etsy-ml-systems-prod.feature_bank_v2.listing_feature_bank_2024-02-08`
where listingWeb_price is not null
and activeListingBasics_priceUsd is null
-- 11543567 have listing web price, not active lisitng price
-- 326248094 reverse


---- Quantity
select count(*)
from `etsy-ml-systems-prod.feature_bank_v2.listing_feature_bank_2024-02-08`
where listingWeb_quantity is not null
and activeListingBasics_quantity is null
-- 11543567 have both fields
-- all equal


---- isFreeShipping
with shipping as (
    select 
        listingWeb_isFreeShipping.key_value[array_length(listingWeb_isFreeShipping.key_value)-1].value as listingWeb_isFreeShipping, 
        activeListingShippingCosts_usShipCost
    from `etsy-ml-systems-prod.feature_bank_v2.listing_feature_bank_2024-02-08`
    where listingWeb_isFreeShipping is not null and activeListingShippingCosts_usShipCost is not null
)
select count(*) from shipping
where activeListingShippingCosts_usShipCost > 0 
and listingWeb_isFreeShipping is True
-- 132810489 have both fields
-- 21003275 isFreeShipping True, usShipCost > 0
-- 14743020 isFreeShipping False, usShipCost = 0



---- More about price
with tmp as (
    select 
        key as listing_id,
        activeListingBasics_priceUsd, 
        activeListingBasics_maxPriceUsd,
        activeListingBasics_minPriceUsd,
        cast(listingWeb_price.key_value[array_length(listingWeb_price.key_value)-1].value as float64) / 100.0 as listing_web_price,
        case
          when array_length(listingWeb_promotionalPrice.key_value) > 0 then cast(listingWeb_promotionalPrice.key_value[array_length(listingWeb_promotionalPrice.key_value)-1].value as float64) / 100.0
          else null
        end as promo_price
    -- select count(*)
    from `etsy-ml-systems-prod.feature_bank_v2.listing_feature_bank_2024-03-05`
    where activeListingBasics_priceUsd is not null
    and activeListingBasics_minPriceUsd is not null and activeListingBasics_maxPriceUsd is not null 
    and activeListingBasics_minPriceUsd != activeListingBasics_maxPriceUsd
    and activeListingBasics_priceUsd > activeListingBasics_maxPriceUsd
)
select count(*) from tmp
where promo_price is not null
and listing_web_price - promo_price < 0
-- 978285316 listings
-- 492049301 has basics price usd
-- 190619636 have basic, min and max price
-- 114659462 (60% of those available) max price != min price (base)

-- 113487413 (99%) price = min price
-- 68649 price = max price
-- 436085 price > min and < max
-- 412760 price < min price
-- 254555 price > max price

-- 254555 listings
-- 26161 promo not null
-- average price diff 57.08
-- min price diff -146.5
-- max price diff 12830.5