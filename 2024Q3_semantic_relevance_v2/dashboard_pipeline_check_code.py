import math
from typing import Callable

import click
import numpy as np
import pandas as pd
import tensorflow as tf
import tensorflow_text  # noqa
from sklearn.metrics import ndcg_score
from google.cloud import bigquery

from semantic_relevance.utils.logs import logger


def get_predict_fn_cern(
    saved_model,
    signature: str = tf.saved_model.DEFAULT_SERVING_SIGNATURE_DEF_KEY,
) -> Callable[[pd.DataFrame], tuple[np.ndarray, np.ndarray]]:
    """Get a predict function which takes in a dataframe
    and returns the class id and softmax output.
    Etsy relevance class ids start at one, so we must +1 to the argmax(softmax)

    Args:
        saved_model: Tensorflow saved model with query and title input
        signature: Model serving signature

    Returns:
        Function to inference relevance score
    """

    def wrapper(df: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
        outputs = saved_model.signatures[signature](
            queries=df["query"].to_numpy(), titles=df["listingTitle"].to_numpy()
        )
        return (tf.math.argmax(outputs["softmax"], axis=-1) + 1).numpy(), outputs["softmax"].numpy()

    return wrapper


def evaluate_model(
    predict_fn: Callable[..., tuple[np.ndarray, np.ndarray]],
    df: pd.DataFrame,
    batch_size: int,
) -> pd.DataFrame:
    """Runs a model predict function on a dataframe of inputs and
    returns a float tensor for each row. Returns a dataframe of scores
    for each query, listing pair. Also includes metadata for each prediction.

    Args:
        predict_fn: Function to run model inference
        df: Dataframe containing batch of query listing data
        batch_size: Size of inference batches

    Returns:
        DataFrame of guid, listingId, and relevance score
    """
    df_batches = np.array_split(df, math.ceil(len(df) / batch_size))

    total_batches = len(df_batches)
    log_interval = (total_batches // 100) or 1
    logger.info(f"Evaluating on {total_batches} batches of {batch_size}")

    metric_dfs = []
    for i, df_batch in enumerate(df_batches):
        if i % log_interval == 0:
            logger.info(f"Running batch {i}/{total_batches}")
        class_id, softmax_scores = predict_fn(df_batch)
        metric_df_batch = pd.DataFrame(
            {
                "query": df_batch["query"],
                "listingTitle": df_batch["listingTitle"],
                "classId": class_id.tolist(),
                "softmaxScores": softmax_scores.tolist(),
            }
        )
        metric_dfs.append(metric_df_batch)

    logger.info("Finished evaluating")

    return pd.concat(metric_dfs)


def load_sampled_requests(client: bigquery.Client, date: str) -> pd.DataFrame:
    sql = f"""
        SELECT tableUUID, guid, query, listingTitle, listingId, pageNum, rankingRank, retrievalRank, bordaRank
        FROM `etsy-data-warehouse-prod.search.sem_rel_hydrated_daily_requests`
        WHERE date = "{date}"
    """
    df = client.query_and_wait(sql).to_dataframe()
    df = df[df["guid"].notna()]
    return df


def create_tables_delete_rows(
    client: bigquery.Client,
    pairs_table_name: str,
    requests_table_name: str,
    date: str,
    model_name: str,
):
    sql = f"""
        CREATE TABLE IF NOT EXISTS `{pairs_table_name}`(
            date DATE,
            modelName STRING,
            tableUUID STRING,
            classId INT64,
            softmaxScores ARRAY<FLOAT64>
        )
        PARTITION BY
            date;
        DELETE FROM `{pairs_table_name}` WHERE date = '{date}' AND modelName = '{model_name}'
    """
    client.query_and_wait(sql)

    sql = f"""
        CREATE TABLE IF NOT EXISTS `{requests_table_name}`(
            date DATE,
            modelName STRING,
            guid STRING,
            relevanceNDCG FLOAT64,
            relevanceNDCG4 FLOAT64,
            relevanceNDCG10 FLOAT64
        )
        PARTITION BY
            date;
        DELETE FROM `{requests_table_name}` WHERE date = '{date}' AND modelName = '{model_name}'
    """
    client.query_and_wait(sql)

    sql = f"""CREATE OR REPLACE VIEW `{pairs_table_name}_vw` AS
        SELECT
            dr.date,
            ql.modelName,
            dr.guid,
            dr.visit_id,
            dr.query,
            dr.listingId,
            dr.pageNum,
            CASE
                WHEN dr.retrievalRank IS NOT NULL THEN "pre-borda"
                WHEN dr.bordaRank IS NOT NULL THEN "post-borda"
                ELSE NULL
            END AS retrievalStage,
            ql.classId,
            ql.softmaxScores
        FROM `etsy-data-warehouse-prod.search.sem_rel_hydrated_daily_requests` dr
        JOIN `{pairs_table_name}` ql
            USING (date, tableUUID);
    """
    client.query_and_wait(sql)

    sql = f"""CREATE OR REPLACE VIEW `{requests_table_name}_vw` AS
        SELECT DISTINCT
            rm.date,
            rm.modelName,
            rm.guid,
            dr.query,
            rm.relevanceNDCG,
            rm.relevanceNDCG4,
            rm.relevanceNDCG10
        FROM `etsy-data-warehouse-prod.search.sem_rel_hydrated_daily_requests` dr
        JOIN `{requests_table_name}` rm
            USING (date, guid);
    """
    client.query_and_wait(sql)


@click.command()
@click.option("--date", type=str, required=True)
@click.option("--model-name", type=str, required=True)
@click.option("--model-path", type=str, required=True)
@click.option("--project-id", type=str, required=True)
@click.option("--batch-size", type=int, default=2048)
@click.option("--pairs-table-name", type=str, required=True)
@click.option("--requests-table-name", type=str, required=True)
def main(
    date, model_name, model_path, project_id, batch_size, pairs_table_name, requests_table_name
):
    logger.info("Starting Evaluation on sampled requests")

    logger.info("Loading model")
    model = tf.saved_model.load(model_path)
    predict_fn = get_predict_fn_cern(model)

    logger.info("Loading sampled requests")
    client = bigquery.Client(project=project_id)
    df = load_sampled_requests(client, date)

    logger.info("Evaluating sampled requests with model")
    df_dedup = df[["query", "listingTitle"]].drop_duplicates()
    df_metrics = evaluate_model(predict_fn, df_dedup, batch_size)

    logger.info("Pairing query-listing metrics dataframe")
    df_scores = pd.merge(df, df_metrics, on=["query", "listingTitle"])
    df_bq_pairs = df_scores[["tableUUID", "classId", "softmaxScores"]]
    df_bq_pairs.insert(loc=0, column="date", value=date)
    df_bq_pairs.insert(loc=1, column="modelName", value=model_name)

    logger.info("Calculating page 1 NDCG")
    df_grouped = (
        df_scores[df_scores["pageNum"] == 1]
        .groupby(by=["guid"])
        .agg({"rankingRank": list, "classId": list})
    )
    guids = []
    ndcgs = []
    ndcg4s = []
    ndcg10s = []
    for row in df_grouped.itertuples():
        if len(row.rankingRank) > 1:
            historical_score = 1.0 - np.array([row.rankingRank])
            relevance_score = (np.array([row.classId]) == 4).astype(np.float32)
            ndcg = ndcg_score(relevance_score, historical_score)
            ndcg4 = ndcg_score(relevance_score, historical_score, k=4)
            ndcg10 = ndcg_score(relevance_score, historical_score, k=10)
            guids.append(row.Index)
            ndcgs.append(ndcg)
            ndcg4s.append(ndcg4)
            ndcg10s.append(ndcg10)

    df_ndcg = pd.DataFrame(
        {
            "guid": guids,
            "relevanceNDCG": ndcgs,
            "relevanceNDCG4": ndcg4s,
            "relevanceNDCG10": ndcg10s,
        }
    )

    logger.info("Pairing requests metrics dataframe")
    df_request = df[["guid"]].drop_duplicates()
    df_bq_request = pd.merge(df_request, df_ndcg, on="guid")
    df_bq_request.insert(loc=0, column="date", value=date)
    df_bq_request.insert(loc=1, column="modelName", value=model_name)

    logger.info(f"Creating tables and deleting data for {model_name} ({date})")
    create_tables_delete_rows(client, pairs_table_name, requests_table_name, date, model_name)

    logger.info(f"Uploading to {pairs_table_name} with {len(df_bq_pairs)} rows")
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("date", bigquery.enums.SqlTypeNames.DATE),
            bigquery.SchemaField("modelName", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("tableUUID", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("classId", bigquery.enums.SqlTypeNames.INTEGER),
            bigquery.SchemaField(
                "softmaxScores", bigquery.enums.SqlTypeNames.FLOAT, mode="REPEATED"
            ),
        ],
        autodetect=False,
    )
    job = client.load_table_from_json(
        df_bq_pairs.to_dict(orient="records"),  # Cannot load array columns via dataframe
        pairs_table_name,
        job_config=job_config,
    )
    job.result()

    logger.info(f"Uploading to {requests_table_name} with {len(df_bq_request)} rows")
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("date", bigquery.enums.SqlTypeNames.DATE),
            bigquery.SchemaField("modelName", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("guid", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("relevanceNDCG", bigquery.enums.SqlTypeNames.FLOAT),
            bigquery.SchemaField("relevanceNDCG4", bigquery.enums.SqlTypeNames.FLOAT),
            bigquery.SchemaField("relevanceNDCG10", bigquery.enums.SqlTypeNames.FLOAT),
        ],
        autodetect=False,
        source_format=bigquery.SourceFormat.CSV,
    )
    job = client.load_table_from_dataframe(
        df_bq_request,
        requests_table_name,
        job_config=job_config,
    )
    job.result()

    logger.info("Completed!")


if __name__ == "__main__":
    main()