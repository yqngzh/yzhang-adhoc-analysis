import time
from datetime import date
import os
from typing import List
import uuid
import pandas as pd
import pyarrow.dataset as ds
from pyarrow.fs import GcsFileSystem
from google.cloud import bigquery

_TEMP_PARQUET_DIR= "gs://training-dev-search-data-jtzn/semantic_relevance/temp/teacher_scores_parquet"

import semantic_relevance.data.listing_feature_bank as data_lfb
from semantic_relevance.utils.bigquery import create_bq_client
from semantic_relevance.utils.logs import logger
from semantic_relevance.utils.constants import (
    TEACHER_V1,
    LABEL_KEY,
    LABEL_PROBA_KEY,
    QUERY_KEY,
    LISTING_ID_KEY,
    LISTING_TITLE_KEY,
    LISTING_TAGS_KEY,
    LISTING_TAXO_KEY,
)


def get_hydrated_teacher_scores(
    start_date: date,
    end_date: date,
    model_name: str = TEACHER_V1,
    extra_features: List[str] = [],
    lfb_query: str = data_lfb.LFB_MOST_RECENT_FEATURES_QUERY,
    order_by_uuid=True,
    limit_postborda=False,
    sample_size=None,
    use_temp_gcs_parquet=False
) -> pd.DataFrame:
    """
    :param start_date: Start date of teacher scores, inclusive
    :param end_date: End date of teacher scores, inclusive
    :param model_name: Teacher model name
    :param extra_features: List of listing features to include in output dataframe from FBv2
    :param lfb_query: SQL query to get features from Listing FBv2
    :param order_by_uuid: If True, order by UUID (so essentialy random ordering)
    :param limit_postborda: If True, only sample post-borda scores
    :param sample_size: Number of instances to sample
    :param use_temp_gcs_parquet: If True, instead of directly using bigquery client to get dataframe:
        - Use bigquery to save results to parquet on GCS
        - Download that parquet to a dataframe with pyarrow
        ... this avoids download size limits on the BigQuery client, and oddly is faster, especially with large results
    :return: Dataframe of scored query-listing pairs from the semantic relevance model score tables
        which power the semantic relevance dashboard (https://looker.etsycloud.com/dashboards/7486?Model+Name=bert-cern-l24-h1024-a16&Date=30+day&Platform=)
        This is used to train student models which distill from more powerful models, and for evaluation against
        the dashboard gold standard
    """
    if len(extra_features) > 0:
        extra_lfb_csv = ",".join(extra_features)
        final_select = f"""
            SELECT modelLabels.* EXCEPT(rowNum),{extra_lfb_csv}
            FROM modelLabels
            LEFT JOIN lfb USING(listingId)
        """
    else:
        final_select = "SELECT * EXCEPT(rowNum) FROM modelLabels"

    if order_by_uuid:
        order_by = " ORDER BY modelLabels.tableUUID DESC"
    else:
        order_by = ""

    if sample_size is not None:
        final_select += f" WHERE modelLabels.rowNum < {sample_size}"

    where_clauses = [
        f"qlm.date BETWEEN '{start_date:%Y-%m-%d}' AND '{end_date:%Y-%m-%d}'",
        f"modelName='{model_name}'",
    ]

    if limit_postborda:
        where_clauses.append("dr.bordaRank IS NOT NULL")

    where_expr = " AND ".join(where_clauses)

    query = f"""
    WITH modelLabels AS (
        SELECT 
          ROW_NUMBER() OVER (ORDER BY qlm.tableUUID DESC) as rowNum,
          qlm.date,
          qlm.tableUUID,
          classId {LABEL_KEY},
          qlm.softmaxScores {LABEL_PROBA_KEY},
          query {QUERY_KEY},
          listingId {LISTING_ID_KEY},
          listingTitle {LISTING_TITLE_KEY},
          listingTags {LISTING_TAGS_KEY},
          listingTaxo {LISTING_TAXO_KEY}
        FROM `etsy-data-warehouse-prod.search.sem_rel_query_listing_metrics` qlm
        JOIN `etsy-data-warehouse-prod.search.sem_rel_hydrated_daily_requests` dr USING (date, tableUUID)
        WHERE {where_expr}
    ), lfb AS (
        {lfb_query}
    )
    {final_select}
    {order_by}
    """
    logger.info(f"Query for hydrated model labels: {query}")

    client: bigquery.Client = create_bq_client()

    if use_temp_gcs_parquet:
        temp_parquet_path = os.path.join(_TEMP_PARQUET_DIR, str(uuid.uuid1()))
        temp_parquet_ptn = os.path.join(temp_parquet_path, "*.parquet")
        logger.info(f"Writing parquet to: {temp_parquet_ptn}")
        export_query = f"""
        EXPORT DATA OPTIONS(
          uri='{temp_parquet_ptn}',
          format='PARQUET',
          overwrite=true
        ) AS
        {query}
        """
        logger.info(f"Query for exporting data to temp parquet: {export_query}")
        start = time.time()
        client.query(export_query).result()
        logger.info(f"Took {time.time() - start} seconds to save results to parquet")

        pa_dset = ds.dataset(temp_parquet_path.removeprefix("gs://"),
          filesystem=GcsFileSystem(),
          format='parquet')
        start = time.time()
        pa_table = pa_dset.to_table()
        # pyarrow dataset can't be directly saved to pandas
        # ideally you could call .to_table with zero memory copy, but this has limited support: https://arrow.apache.org/docs/python/pandas.html#reducing-memory-use-in-table-to-pandas
        # split_blocks and self_destruct are used to reduce memory duplication: https://arrow.apache.org/docs/python/pandas.html#reducing-memory-use-in-table-to-pandas
        df = pa_table.to_pandas(split_blocks=True, self_destruct=True)
        logger.info(f"Took {time.time() - start} seconds to load parquet into dataframe")
        del pa_table
        return df

    start_time = time.time()
    client: bigquery.Client = create_bq_client()
    df = client.query(query).to_dataframe()
    # if sample_size is not None:
    #     if sample_size > len(df):
    #         logger.warn(f"Sample size {sample_size} more than size of teacher scores {len(df)}")
    #         sample_size = len(df)
    #     df = df.sample(n=sample_size, random_state=42)
    logger.info(f"Took {time.time() - start_time} seconds to get hydrated model labels")
    return df