import logging
import time
import argparse
from typing import List
import numpy as np
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import bigquery

# from analyze_embedding import GetEmbeddingAndSimilarity, run_query_df, BQ_SQL, input_data


BQ_SQL = """select distinct
    requestUUID, position,
    ctx.docInfo.queryInfo.query as query_str,
    ctx.docInfo.queryInfo.queryLevelMetrics.bin as query_bin,
    candidateInfo.docInfo.listingInfo.listingId as listing_id,
from `etsy-ml-systems-prod.attributed_instance.query_pipeline_web_organic_2023_08_29`,
    unnest(contextualInfo) as ctx
where ctx.docInfo.queryInfo.query is not null
and candidateInfo.docInfo.listingInfo.listingId is not null
order by requestUUID, position
"""

# input_data = [
#     {
#         "requestUUID": "aaa",
#         "position": 0,
#         "query_str": "test1",
#         "query_bin": "head",
#         "listing_id": 902792488,
#     },
#     {
#         "requestUUID": "aaa",
#         "position": 1,
#         "query_str": "test1",
#         "query_bin": "head",
#         "listing_id": 725104244,
#     },
#     {
#         "requestUUID": "aaa",
#         "position": 2,
#         "query_str": "test1",
#         "query_bin": "head",
#         "listing_id": 1221932302,
#     },
#     {
#         "requestUUID": "bbb",
#         "position": 0,
#         "query_str": "xxx",
#         "query_bin": "top01",
#         "listing_id": 793904541,
#     },
#     {
#         "requestUUID": "bbb",
#         "position": 1,
#         "query_str": "xxx",
#         "query_bin": "top01",
#         "listing_id": 795642038,
#     },
#     {
#         "requestUUID": "bbb",
#         "position": 2,
#         "query_str": "xxx",
#         "query_bin": "top01",
#         "listing_id": 1323065344,
#     },
#     {
#         "requestUUID": "ccc",
#         "position": 0,
#         "query_str": "random",
#         "query_bin": "head",
#         "listing_id": 855096617,
#     },
#     {
#         "requestUUID": "ccc",
#         "position": 1,
#         "query_str": "random",
#         "query_bin": "head",
#         "listing_id": 1007337105,
#     },
#     {
#         "requestUUID": "ccc",
#         "position": 2,
#         "query_str": "random",
#         "query_bin": "head",
#         "listing_id": 1506329131,
#     },
# ]


def run_query_df(query_str, project_id="etsy-bigquery-adhoc-prod"):
    client = bigquery.Client(project=project_id)
    query_job = client.query(query_str)
    rows = query_job.result()
    df = rows.to_dataframe()
    return df


def convert_bq_beam_schema(schema):
    output = []
    for item in schema:
        item_dict = item.to_api_repr()
        output.append(f"{item_dict['name']}:{item_dict['type']}")
    return ",".join(output)


class GetEmbeddingAndSimilarity(beam.DoFn):
    def compute_similarity(self, embeddings: List[List[float]]):
        embed_array = np.array(embeddings)
        data_normed = embed_array / np.linalg.norm(embed_array, axis=1, keepdims=True)
        cosine_sim_matrix = data_normed @ data_normed.T
        return list(cosine_sim_matrix.flatten())

    def _get_vsv2_embeddings(self, listing_ids: List[int]) -> List[List[float]]:
        qstr = f"""select key as listing_id, VSV2Embeddings_vsv2Embedding512.list as embedding
        from `etsy-ml-systems-prod.feature_bank_v2.listing_feature_bank_most_recent`
        where key in ({','.join([str(x) for x in listing_ids])})
        """
        df = run_query_df(qstr)
        df["embedding"] = df.embedding.apply(
            lambda lst: [item["element"] for item in lst]
        )
        df = df.sort_values(
            by="listing_id", key=lambda col: [listing_ids.index(x) for x in col]
        )
        assert np.all(df.listing_id.values == listing_ids)
        return list(df.embedding)

    def process(self, row):
        in_data = row[1]
        out_data = {
            "requestUUID": in_data[0]["requestUUID"],
            "query_str": in_data[0]["query_str"],
            "query_bin": in_data[0]["query_bin"],
            "listing_ids": [],
        }
        try:
            prev_position = -1
            for i in range(len(in_data)):
                assert in_data[i]["position"] > prev_position
                prev_position = in_data[i]["position"]
                out_data["listing_ids"].append(in_data[i]["listing_id"])
            embeddings = self._get_vsv2_embeddings(out_data["listing_ids"])
            out_data["cosine_sim"] = self.compute_similarity(embeddings)
            return [out_data]
        except:
            logging.warning(f"Request {in_data[0]['requestUUID']} has missing listing")


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output_table",
        required=True,
        help="Output table to write results to",
    )
    parser.add_argument(
        "--output_dir",
        required=True,
        help="Output file to write results to",
    )
    args, _ = parser.parse_known_args(argv)

    now = str(int(time.time()))
    pipeline_options = PipelineOptions(
        save_main_session=True,
        pipeline_type_check=True,
        job_name=f"yzhang-visual-diversity-embedding-process-{now}",
    )

    output_schema = [
        bigquery.SchemaField("requestUUID", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("query_str", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("query_bin", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField(
            "listing_ids", bigquery.enums.SqlTypeNames.INT64, mode="REPEATED"
        ),
        bigquery.SchemaField(
            "cosine_sim", bigquery.enums.SqlTypeNames.FLOAT64, mode="REPEATED"
        ),
    ]
    beam_output_schema = convert_bq_beam_schema(output_schema)

    with beam.Pipeline(options=pipeline_options) as pipeline:
        # with beam.Pipeline() as pipeline:
        (
            pipeline
            # | "Create" >> beam.Create(input_data)
            | "Read input data"
            >> beam.io.ReadFromBigQuery(
                query=BQ_SQL,
                use_standard_sql=True,
                gcs_location=f"{args.output_dir}/bq",
            )
            | "Group by request" >> beam.GroupBy(lambda x: x["requestUUID"])
            | "Get embedding" >> beam.ParDo(GetEmbeddingAndSimilarity())
            # | "Print" >> beam.Map(print)
            | "Write results to BigQuery"
            >> beam.io.WriteToBigQuery(
                args.output_table,
                schema=beam_output_schema,
                method="FILE_LOADS",
            )
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
