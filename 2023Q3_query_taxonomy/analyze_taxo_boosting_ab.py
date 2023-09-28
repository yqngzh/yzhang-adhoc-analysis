import logging
import time
import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.internal.clients.bigquery import TableSchema
from apache_beam.io.gcp.internal.clients.bigquery import TableFieldSchema
from google.cloud import bigquery


class DemandProcess(beam.DoFn):
    def process(self, row):
        out_data = {
            k: v
            for k, v in row.items()
            if k
            not in [
                "click_top_taxo",
                "click_level2_taxo",
                "purchase_top_taxo",
                "purchase_level2_taxo",
            ]
        }
        listing_full_path = row["full_path"]
        if (
            (listing_full_path is None)
            or (row["click_top_taxo"] is None)
            or (row["click_level2_taxo"] is None)
            or (row["purchase_top_taxo"] is None)
            or (row["purchase_level2_taxo"] is None)
        ):
            # no listing information, or any click, purchase null, assume not overlapping
            out_data["click_top_overlap"] = 0
            out_data["click_level2_overlap"] = 0
            out_data["purchase_top_overlap"] = 0
            out_data["purchase_level2_overlap"] = 0
        else:
            # logging.warning(row["click_top_taxo"])
            # {'list': [{'element': 'home_and_living'}, {'element': 'art_and_collectibles'}]}
            query_click_top_node = [
                item["element"] for item in row["click_top_taxo"]["list"]
            ]
            query_click_level2_node = [
                item["element"]
                for item in row["click_level2_taxo"]["list"]
                if item["element"] != ""
            ]
            query_purchase_top_node = [
                item["element"] for item in row["purchase_top_taxo"]["list"]
            ]
            query_purchase_level2_node = [
                item["element"]
                for item in row["purchase_level2_taxo"]["list"]
                if item["element"] != ""
            ]
            listing_full_path_split = listing_full_path.split(".")
            if len(listing_full_path_split) > 1:
                # listing has at least 2 levels of taxonomy nodes
                listing_top_node = listing_full_path_split[0]
                listing_level2_node = (
                    listing_full_path_split[0] + "." + listing_full_path_split[1]
                )
                out_data["click_top_overlap"] = (
                    1 if listing_top_node in query_click_top_node else 0
                )
                out_data["click_level2_overlap"] = (
                    1 if listing_level2_node in query_click_level2_node else 0
                )
                out_data["purchase_top_overlap"] = (
                    1 if listing_top_node in query_purchase_top_node else 0
                )
                out_data["purchase_level2_overlap"] = (
                    1 if listing_level2_node in query_purchase_level2_node else 0
                )
            else:
                # listing only has top taxonomy node
                out_data["click_top_overlap"] = (
                    1 if listing_full_path in query_click_top_node else 0
                )
                out_data["purchase_top_overlap"] = (
                    1 if listing_full_path in query_purchase_top_node else 0
                )
                out_data["click_level2_overlap"] = 0
                out_data["purchase_level2_overlap"] = 0
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
        job_name=f"yzhang-taxo-boost-ab-analysis-{now}",
    )

    client = bigquery.Client(project=pipeline_options.get_all_options()["project"])
    in_table = client.get_table(args.input_table)
    in_schema_bq = in_table.schema
    in_schema_bq_reduced = [
        item
        for item in in_schema_bq
        if item.name
        not in [
            "click_top_taxo",
            "click_level2_taxo",
            "purchase_top_taxo",
            "purchase_level2_taxo",
        ]
    ]
    output_schema_bq = in_schema_bq_reduced + [
        bigquery.SchemaField("click_top_overlap", bigquery.enums.SqlTypeNames.INT64),
        bigquery.SchemaField("purchase_top_overlap", bigquery.enums.SqlTypeNames.INT64),
        bigquery.SchemaField("click_level2_overlap", bigquery.enums.SqlTypeNames.INT64),
        bigquery.SchemaField(
            "purchase_level2_overlap", bigquery.enums.SqlTypeNames.INT64
        ),
    ]
    out_table = bigquery.Table(
        args.output_table.replace(":", "."), schema=output_schema_bq
    )
    client.create_table(out_table, exists_ok=True)

    output_schema_beam = TableSchema()
    for item in output_schema_bq:
        field_schema = TableFieldSchema()
        field_schema.name = item.name
        field_schema.type = item.field_type
        field_schema.mode = item.mode
        output_schema_beam.fields.append(field_schema)

    with beam.Pipeline(options=pipeline_options) as pipeline:
        # with beam.Pipeline() as pipeline:
        (
            pipeline
            # | "Create" >> beam.Create(input_data)
            | "Read input data"
            >> beam.io.ReadFromBigQuery(
                query=f"select * from `{args.input_table}`",
                # query="select * from `etsy-sr-etl-prod.yzhang.query_taxo_web_full` limit 100",
                use_standard_sql=True,
                gcs_location=f"gs://etldata-prod-search-ranking-data-hkwv8r/data/shared/tmp",
            )
            # | "Print" >> beam.Map(print)
            | "Data summary" >> beam.ParDo(DemandProcess())
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
