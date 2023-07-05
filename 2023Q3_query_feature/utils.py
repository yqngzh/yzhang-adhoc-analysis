from typing import List
from google.cloud import bigquery

def run_query_df(query_str, project_id="etsy-bigquery-adhoc-prod"):
    client = bigquery.Client(project=project_id)
    query_job = client.query(query_str)
    rows = query_job.result()
    df = rows.to_dataframe()
    return df


def get_listing_taxonomy(listing_ids: List):
    query_str = f"""
    select listing_id, full_path
    from `etsy-data-warehouse-prod.materialized.listing_taxonomy`
    where listing_id in ({",".join([str(x) for x in listing_ids])})
    """
    df = run_query_df(query_str)
    df["tax_top1_node"] = df.full_path.apply(lambda x: x.split(".")[0] if len(x.split(".")) > 1 else x)
    df["tax_top2_node"] = df.full_path.apply(lambda x: x.split(".")[1] if len(x.split(".")) > 1 else None)
    return df


def get_clipvit_joint_embedding(listing_ids: List):
    pass
