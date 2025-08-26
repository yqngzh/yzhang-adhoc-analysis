-- impressed page 1 from US web sign-out
-- queries that's consistently bad across all retrieved page 1

-- get web US SO visits whose first event is a search event, get listings on page 1 
with first_event_of_visit_web_us_so as (
    select
        visit_id,
        browser_id,
        event_timestamp,
        source_request_uuid as mmx_request_uuid,
        request_time,
        query,
        listing_id,
        listing_position,
        clicked_flag as has_click,
        in_visit_same_listing_last_click_purchase_flag as has_last_click_tight_purchase,
        source,
        placement,
        page_number,
    from `etsy-data-warehouse-prod.rollups.unified_impressions`
    where _date between date('2025-07-06') and date('2025-08-23')
    and event_source = "web"
    and platform = "desktop"
    and buyer_segment = "Signed Out"
    and buyer_country = "US"
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY visit_id ORDER BY event_timestamp ASC, request_time ASC, source_request_uuid ASC
    ) = 1
),
first_event_search_page1_web_us_so as (
    select 
        visit_id,
        browser_id,
        event_timestamp,
        mmx_request_uuid,
        request_time,
        query,
        listing_id,
        listing_position,
        has_click,
        has_last_click_tight_purchase,
    from first_event_of_visit_web_us_so
    where source = 'search'
    and placement = "wsg"
    and page_number = 1
    and listing_position < 48
    and query is not null and query != ""
)
select distinct mmx_request_uuid, query, listing_id, listing_position
from first_event_search_page1_web_us_so
order by mmx_request_uuid, query, listing_position
--- strangely, each request only has a few impressed listings rather than first row etc