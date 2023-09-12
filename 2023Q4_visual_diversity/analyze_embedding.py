import logging
import time
import argparse
from typing import List
import numpy as np
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.internal.clients.bigquery import TableSchema
from apache_beam.io.gcp.internal.clients.bigquery import TableFieldSchema
from google.cloud import bigquery


# def run_query_df(query_str, project_id="etsy-bigquery-adhoc-prod"):
#     client = bigquery.Client(project=project_id)
#     query_job = client.query(query_str)
#     rows = query_job.result()
#     df = rows.to_dataframe()
#     return df


# def convert_bq_beam_schema(schema):
#     output = []
#     for item in schema:
#         item_dict = item.to_api_repr()
#         output.append(f"{item_dict['name']}:{item_dict['type']}")
#     return ",".join(output)


class GetEmbeddingAndSimilarity(beam.DoFn):
    def compute_similarity(self, embeddings: List[List[float]]):
        if len(embeddings) > 0:
            embed_array = np.array(embeddings)
            data_normed = embed_array / np.linalg.norm(
                embed_array, axis=1, keepdims=True
            )
            cosine_sim_matrix = data_normed @ data_normed.T
            return list(cosine_sim_matrix.flatten())
        else:
            return []

    def process(self, row):
        in_data = row[1]
        out_data = {
            "requestUUID": in_data[0]["requestUUID"],
            "visitId": in_data[0]["visitId"],
            "query_str": in_data[0]["query_str"],
            "query_bin": in_data[0]["query_bin"],
        }

        new_listing_ids, positions, embeddings = [], [], []
        for i in range(len(in_data)):
            curr_in_data = in_data[i]
            if (curr_in_data["embedding"] is not None) and (
                len(curr_in_data["embedding"]) > 0
            ):
                new_listing_ids.append(curr_in_data["listing_id"])
                embeddings.append(
                    [item["element"] for item in curr_in_data["embedding"]]
                )
                positions.append(curr_in_data["position"])

        # sort by position
        zipped_data = sorted(zip(positions, new_listing_ids, embeddings))
        new_listing_ids_sorted = [item[1] for item in zipped_data]
        embeddings_sorted = [item[2] for item in zipped_data]

        out_data["listing_ids"] = new_listing_ids_sorted
        out_data["cosine_sim"] = self.compute_similarity(embeddings_sorted)
        return [out_data]


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_table",
        required=True,
        help="Input table with requests and embeddings",
    )
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

    output_schema_beam = TableSchema()

    field_schema = TableFieldSchema()
    field_schema.name = "requestUUID"
    field_schema.type = "STRING"
    field_schema.mode = "NULLABLE"
    output_schema_beam.fields.append(field_schema)

    field_schema = TableFieldSchema()
    field_schema.name = "visitId"
    field_schema.type = "STRING"
    field_schema.mode = "NULLABLE"
    output_schema_beam.fields.append(field_schema)

    field_schema = TableFieldSchema()
    field_schema.name = "query_str"
    field_schema.type = "STRING"
    field_schema.mode = "NULLABLE"
    output_schema_beam.fields.append(field_schema)

    field_schema = TableFieldSchema()
    field_schema.name = "query_bin"
    field_schema.type = "STRING"
    field_schema.mode = "NULLABLE"
    output_schema_beam.fields.append(field_schema)

    field_schema = TableFieldSchema()
    field_schema.name = "listing_ids"
    field_schema.type = "INT64"
    field_schema.mode = "REPEATED"
    output_schema_beam.fields.append(field_schema)

    field_schema = TableFieldSchema()
    field_schema.name = "cosine_sim"
    field_schema.type = "FLOAT64"
    field_schema.mode = "REPEATED"
    output_schema_beam.fields.append(field_schema)

    output_schema_bq = [
        bigquery.SchemaField("requestUUID", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("visitId", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("query_str", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("query_bin", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField(
            "listing_ids", bigquery.enums.SqlTypeNames.INT64, mode="REPEATED"
        ),
        bigquery.SchemaField(
            "cosine_sim", bigquery.enums.SqlTypeNames.FLOAT64, mode="REPEATED"
        ),
    ]

    table = bigquery.Table(args.output_table.replace(":", "."), schema=output_schema_bq)
    client = bigquery.Client(project=pipeline_options.get_all_options()["project"])
    client.create_table(table, exists_ok=True)

    with beam.Pipeline(options=pipeline_options) as pipeline:
        # with beam.Pipeline() as pipeline:
        (
            pipeline
            # | "Create" >> beam.Create(input_data)
            | "Read input data"
            >> beam.io.ReadFromBigQuery(
                query=f"select * from `{args.input_table}` order by requestUUID, position",
                use_standard_sql=True,
                gcs_location=f"gs://etldata-prod-search-ranking-data-hkwv8r/data/shared/tmp",
            )
            | "Group by request" >> beam.GroupBy(lambda x: x["requestUUID"])
            | "Get embedding similarity" >> beam.ParDo(GetEmbeddingAndSimilarity())
            # | "Print" >> beam.Map(print)
            | "Write results to BigQuery"
            >> beam.io.WriteToBigQuery(
                args.output_table,
                schema=output_schema_beam,
                method="FILE_LOADS",
            )
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
