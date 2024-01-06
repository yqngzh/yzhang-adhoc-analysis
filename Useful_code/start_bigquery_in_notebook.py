# !gcloud config set project etsy-bigquery-adhoc-prod
# %load_ext google.cloud.bigquery

# %%bigquery df
# select distinct query, visit_id, attributed_gms as gms
# from `etsy-data-warehouse-prod.search.query_sessions_new`
# where _date = date('2023-09-08')
