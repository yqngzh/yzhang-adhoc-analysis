with boe_fl as (
  select 
      requestUUID, 
      attributions, 
      ctx.rivuletUserInfo.userId,
      ctx.rivuletUserInfo.timeseries.recentlySearchedQueries50FV1,
      ctx.rivuletUserInfo.timeseries.recentlyViewedListingIds50FV1,
      ctx.userInfo.tfIdf.recentClickTfIdfVector,
      candidateInfo.docInfo.listingInfo.listingId
  from `etsy-ml-systems-prod.attributed_instance.query_pipeline_boe_organic_2023_11_05`,
      unnest(contextualInfo) as ctx
  where ctx.docInfo.queryInfo.query is not null
)
select 