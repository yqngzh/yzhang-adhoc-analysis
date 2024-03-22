create or replace table `etsy-sr-etl-prod.yzhang.lsig_tire_web_pow0506` as (
    select
        response.mmxRequestUUID as tireRequestUUID,
        request.OPTIONS.personalizationOptions.userId,
        request.query,
        listing_id,
        CAST(request.offset / request.limit + 1 AS INTEGER) page_no,
        tireRequestContext.variant,
        position
    from `etsy-searchinfra-gke-dev.thrift_tire_listingsv2search_search.rpc_logs_*`,
        UNNEST(response.listingIds) AS listing_id  WITH OFFSET position
    where tireRequestContext.tireTestv2Id = "KpktT68edxTCSZTlKD1M"
    and request.query != ""
    and response.mmxRequestUUID is not null
    and request.limit != 0
)

create or replace table `etsy-sr-etl-prod.yzhang.lsig_tire_analysis_web_pow0506` as (
    with query_data as (
        select 
            `key` as query_str,
            queryLevelMetrics_bin as query_bin,
            queryLevelMetrics_isDigital as query_is_digital
        from `etsy-ml-systems-prod.feature_bank_v2.query_feature_bank_2024-03-18`
    ),
    user_data as (
        select `key` as user_id, 
            userSegmentFeatures_buyerSegment as buyer_segment
        from `etsy-ml-systems-prod.feature_bank_v2.user_feature_bank_2024-03-18`
    ),
    listing_data as (
        select `key` as listing_id, 
            listingWeb_isDigital as listing_is_digital,
            activeListingBasics_priceUsd as alb_price,
            cast(listingWeb_price.key_value[array_length(listingWeb_price.key_value)-1].value as float64) / 100.0 as listing_web_price, 
            case
                when array_length(listingWeb_promotionalPrice.key_value) > 0 then cast(listingWeb_promotionalPrice.key_value[array_length(listingWeb_promotionalPrice.key_value)-1].value as float64) / 100.0
                else null
            end as promo_price
        from `etsy-ml-systems-prod.feature_bank_v2.listing_feature_bank_2024-03-18`
    )
    select
        tire_res.*,
        if (tire_res.userId > 0, udata.buyer_segment, "Signed Out") as buyer_segment,
        qdata.query_bin,
        qdata.query_is_digital,
        ldata.listing_is_digital,
        if (ldata.promo_price is not null, ldata.promo_price, ldata.listing_web_price) as lweb_real_price,
        ldata.alb_price
    from `etsy-sr-etl-prod.yzhang.lsig_tire_web_pow0506` tire_res
    left join query_data qdata
    on tire_res.query = qdata.query_str
    left join listing_data ldata
    on tire_res.listing_id = ldata.listing_id
    left join user_data udata
    on tire_res.userId = udata.user_id
    where page_no <= 3
)


with tmp as (
    select 
        if (lweb_real_price is not null, lweb_real_price, alb_price) as realPriceUsd,
        variant,
        query_bin,
        buyer_segment,
        query_is_digital,
        listing_is_digital
    from `etsy-sr-etl-prod.yzhang.lsig_tire_analysis_web_pow0506`
    -- where page_no = 1
)
select variant, buyer_segment, avg(realPriceUsd) as avg_realPriceUsd
from tmp
group by variant, buyer_segment
order by variant, buyer_segment


select count(*)
from `etsy-sr-etl-prod.yzhang.lsig_tire_analysis_web_sqrt`
where lweb_real_price is null and alb_price is not null
-- total: 10547542
-- price different: 6792210
-- price differ at least $5: 2603783 (25%)
-- 284040 listing web price larger


---- Sanity check for listing web price parsing
with tmp as (
  select `key` as listing_id, 
    listingWeb_isDigital as listing_is_digital,
    activeListingBasics_priceUsd as alb_price,
    listingWeb_price,
    cast(listingWeb_price.key_value[array_length(listingWeb_price.key_value)-1].value as float64) / 100.0 as listing_web_price, 
    listingWeb_promotionalPrice,
    case
        when array_length(listingWeb_promotionalPrice.key_value) > 0 then cast(listingWeb_promotionalPrice.key_value[array_length(listingWeb_promotionalPrice.key_value)-1].value as float64) / 100.0
        else null
    end as promo_price
from `etsy-ml-systems-prod.feature_bank_v2.listing_feature_bank_2024-03-18`
)
select * 
from tmp
where promo_price is not null
and alb_price is not null
limit 20