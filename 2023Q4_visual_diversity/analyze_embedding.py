import logging
import time
import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.internal.clients.bigquery import TableSchema
from apache_beam.io.gcp.internal.clients.bigquery import TableFieldSchema
from google.cloud import bigquery

from analyze_embedding_utils import GetEmbeddingAndSimilarity

# Run the following in B
# create table `etsy-sr-etl-prod.yzhang.visual_diversity_vsv2_embedding` as (
#   with train_data as (
#     select distinct
#         requestUUID, position,
#         ctx.docInfo.queryInfo.query as query_str,
#         ctx.docInfo.queryInfo.queryLevelMetrics.bin as query_bin,
#         candidateInfo.docInfo.listingInfo.listingId as listing_id,
#     from `etsy-ml-systems-prod.attributed_instance.query_pipeline_web_organic_2023_09_08`,
#         unnest(contextualInfo) as ctx
#     where ctx.docInfo.queryInfo.query is not null
#     and candidateInfo.docInfo.listingInfo.listingId is not null
#   ),
#   fb_data as (
#     select key as listing_id, VSV2Embeddings_vsv2Embedding512.list as embedding
#     from `etsy-ml-systems-prod.feature_bank_v2.listing_feature_bank_most_recent`
#     where key in (
#       select distinct listing_id from train_data
#     )
#   )
#   select train_data.*, fb_data.embedding
#   from train_data
#   left join fb_data
#   on train_data.listing_id = fb_data.listing_id
#   order by requestUUID, position
# )
# ```


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

    output_schema_beam = TableSchema()

    field_schema = TableFieldSchema()
    field_schema.name = "requestUUID"
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
                query="select * from `etsy-sr-etl-prod.yzhang.visual_diversity_vsv2_embedding` limit 200",
                use_standard_sql=True,
                gcs_location=f"gs://etldata-prod-search-ranking-data-hkwv8r/data/shared/tmp/bq_load",
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
