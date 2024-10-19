create or replace table `etsy-sr-etl-prod.yzhang.semrel-v2-tire-si-swap-only` as (
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
        where tireRequestContext.tireTestv2Id = "YqkKh1Mc32mASwJK1owS"
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
---- si-swap-only: YqkKh1Mc32mASwJK1owS
---- si-swap-only-run2: Z5rUKcHdGQtlL4aR2iiE
---- so-swap-only: p57DjJwFVmsLUpVn4jOA


with grouped as (
    select 
        page_no,
        IFNULL(buyer_segment, "Unknown") AS segment,
        variant,
        SUM(count_uuid) AS num_request,
        AVG(avg_price_per_uuid) as avg_price,
    from `etsy-sr-etl-prod.yzhang.semrel-v2-tire-si-swap-only`
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
    from `etsy-sr-etl-prod.yzhang.semrel-v2-tire-si-swap-only`
    where page_no = 1
    group by variant, page_no
)
select * from grouped
order by variant

