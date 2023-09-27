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


input_data = [
    {
        "full_path": "pet_supplies.pet_clothing_accessories_and_shoes.pet_accessories.pet_neckwear",
        "click_top_taxo": [
            {"element": "home_and_living"},
            {"element": "art_and_collectibles"},
        ],
        "click_taxo": [
            {"element": "art_and_collectibles.prints"},
            {"element": "home_and_living.home_decor"},
        ],
    }
]


class DemandProcess(beam.DoFn):
    def process(self, row):
        listing_full_path = row["full_path"]
        if listing_full_path is None or len(listing_full_path) == 0:
            # no listing information, assume not overlapping
            row["click_top_overlap"] = 0
            row["click_level2_overlap"] = 0
            row["purchase_top_overlap"] = 0
            row["purchase_level2_overlap"] = 0
        else:
            query_click_top_node = [item["element"] for item in row["click_top_taxo"]]
            query_click_level2_node = [
                item["element"] for item in row["click_level2_taxo"]
            ]
            query_purchase_top_node = [
                item["element"] for item in row["purchase_top_taxo"]
            ]
            query_purchase_level2_node = [
                item["element"] for item in row["purchase_level2_taxo"]
            ]

            listing_full_path_split = listing_full_path.split(".")
            if len(listing_full_path_split) > 1:
                # listing has at least 2 levels of taxonomy nodes
                listing_top_node = listing_full_path_split[0]
                listing_level2_node = (
                    listing_full_path_split[0] + "." + listing_full_path_split[1]
                )
                row["click_top_overlap"] = (
                    1 if listing_top_node in query_click_top_node else 0
                )
                row["click_level2_overlap"] = (
                    1 if listing_level2_node in query_click_level2_node else 0
                )
                row["purchase_top_overlap"] = (
                    1 if listing_top_node in query_purchase_top_node else 0
                )
                row["purchase_level2_overlap"] = (
                    1 if listing_level2_node in query_purchase_level2_node else 0
                )
            else:
                # listing only has top taxonomy node
                row["click_top_overlap"] = (
                    1 if listing_full_path in query_click_top_node else 0
                )
                row["purchase_top_overlap"] = (
                    1 if listing_full_path in query_purchase_top_node else 0
                )
                row["click_level2_overlap"] = 0
                row["purchase_level2_overlap"] = 0


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
        job_name=f"yzhang-taxo-boost-ab-analysis-{now}",
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

    # with beam.Pipeline(options=pipeline_options) as pipeline:
    with beam.Pipeline() as pipeline:
        (
            pipeline
            | "Create" >> beam.Create(input_data)
            # | "Read input data"
            # >> beam.io.ReadFromBigQuery(
            #     # query=f"select * from `{args.input_table}`",
            #     query="select * from `etsy-sr-etl-prod.yzhang.query_taxo_web_full` limit 1",
            #     use_standard_sql=True,
            #     gcs_location=f"gs://etldata-prod-search-ranking-data-hkwv8r/data/shared/tmp",
            # )
            | "Print" >> beam.Map(print)
            # | "Data summary" >> beam.ParDo(DemandProcess())
            # | "Write results to BigQuery"
            # >> beam.io.WriteToBigQuery(
            #     args.output_table,
            #     schema=output_schema_beam,
            #     method="FILE_LOADS",
            # )
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
