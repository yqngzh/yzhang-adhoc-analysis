create or replace table `etsy-sr-etl-prod.yzhang.sem-rel-pw-tire-web-pow0306` as (
    with tire_requests as (
        select
            response.mmxRequestUUID as tireRequestUUID,
            (SELECT value FROM UNNEST(request.context) WHERE key = 'uuid') AS uuid,
            request.OPTIONS.personalizationOptions.userId,
            request.query,
            CAST(request.offset / request.limit + 1 AS INTEGER) page_no,
            tireRequestContext.variant,
            solrScore.listingId,
            solrScore.priceNative,
            pos as tire_position
        from `etsy-searchinfra-gke-dev.thrift_tire_listingsv2search_search.rpc_logs_*`,
            UNNEST(response.solrScores) as solrScore WITH OFFSET pos
        where tireRequestContext.tireTestv2Id = "wTsd7S3O5mAIkSeNaBXd"
        and request.query != ""
        and response.mmxRequestUUID is not null
        and request.limit != 0
    ),
    rpc_segment as (
        select
          t.tireRequestUUID,
          t.variant,
          t.page_no,
          u.buyer_segment,
          q.bin AS query_bin,
          t.priceNative / 100 AS priceUsd
        from tire_requests t
        left join `etsy-data-warehouse-prod.arizona.user_buyer_segment` u
        on t.userId = u.user_id
        left join `etsy-data-warehouse-prod.search.query_bins` q
        on t.query = q.query_raw
        where t.page_no <= 4
    )
    select 
        tireRequestUUID,
        variant,
        page_no,
        buyer_segment,
        query_bin,
        avg(priceUsd) AS avg_price_per_uuid,
        count(*) AS count_uuid
    from rpc_segment
    group by tireRequestUUID, variant, page_no, buyer_segment, query_bin
);


with grouped as (
    select 
        page_no,
        IFNULL(buyer_segment, "Unknown") AS segment,
        variant,
        SUM(count_uuid) AS num_request,
        AVG(avg_price_per_uuid) as avg_price,
    from `etsy-sr-etl-prod.yzhang.sem-rel-pw-tire-web-pow0306`
    where page_no = 1
    group by variant, segment, page_no
)
select * from grouped
order by segment, variant


with grouped as (
    select 
        page_no,
        variant,
        SUM(count_uuid) AS num_request,
        AVG(avg_price_per_uuid) as avg_price,
    from `etsy-sr-etl-prod.yzhang.sem-rel-pw-tire-web-pow0306`
    where page_no = 1
    group by variant, page_no
)
select * from grouped
order by variant



-- boe online impression price change
with exp_data as (
    select variant_id, listingId, 
    from `etsy-data-warehouse-dev.search.sr-sem-rel-v1-boe_query-listing-metrics_vw`
    where pageNum = 1
),
listing_price as (
    select key as listingId, activeListingBasics_priceUsd as priceUsd
    from `etsy-ml-systems-prod.feature_bank_v2.listing_feature_bank_most_recent`
),
joined as (
    select e.variant_id, e.listingId, priceUsd
    from exp_data e
    left join listing_price lp
    on e.listingId = lp.listingId
)
select variant_id, avg(priceUsd) as avg_price
from joined
group by variant_id
-- control 53.998836
-- test 54.067742