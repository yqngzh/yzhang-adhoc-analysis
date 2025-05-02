-- shop name queries
with selected_shops as (
  select shop_id 
  from `etsy-data-warehouse-prod.arizona.shop_gms_1y`
  order by gms_py desc
  limit 100
)
select distinct name, sd.shop_id
from `etsy-data-warehouse-prod.etsy_shard.shop_data` sd
join selected_shops
on selected_shops.shop_id = sd.shop_id
and status = "active"


-- sr-goldquery with listing titles
create or replace table `etsy-sr-etl-prod.yzhang.bert_ce_gold_query_set_title` as (
  with joined as (
    SELECT gq.query, fb.verticaListings_title as listing_title, gq.listing_id, gq.shop_id 
    FROM `etsy-sr-etl-prod.yzhang.bert_ce_gold_query_set` gq
    left join `etsy-ml-systems-prod.feature_bank_v2.listing_feature_bank_2024-06-10` fb
    on gq.listing_id = fb.key
  )
  select * from joined
  where listing_title is not NULL
  and listing_title != ''
)

