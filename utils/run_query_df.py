from typing import List
from google.cloud import bigquery


def run_query_df(query_str, project_id="etsy-bigquery-adhoc-prod"):
    client = bigquery.Client(project=project_id)
    query_job = client.query(query_str)
    rows = query_job.result()
    df = rows.to_dataframe()
    return df
