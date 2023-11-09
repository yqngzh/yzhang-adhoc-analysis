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
                "purchase_top_taxo",
                "purchase_level2_taxo",
            ]
        }

        query_top_taxo = row["purchase_top_taxo"]
        listing_top_taxo = row["listing_top_category"]

        if query_top_taxo is not None and listing_top_taxo is not None:
            query_top_taxo = [item["element"] for item in query_top_taxo["list"]]
            out_data["purchase_top_taxo"] = query_top_taxo
            if listing_top_taxo in query_top_taxo:
                out_data["top_overlap"] = 1
        if "top_overlap" not in out_data:
            out_data["top_overlap"] = 0

        query_level2_taxo = row["purchase_level2_taxo"]
        listing_level2_taxo = row["listing_level2_category"]

        if query_level2_taxo is not None and listing_level2_taxo is not None:
            query_level2_taxo = [item["element"] for item in query_level2_taxo["list"]]
            if listing_level2_taxo in query_level2_taxo:
                out_data["level2_overlap"] = 1
        if "level2_overlap" not in out_data:
            out_data["level2_overlap"] = 0

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
            "purchase_top_taxo",
            "purchase_level2_taxo",
        ]
    ]

    output_schema_bq = in_schema_bq_reduced + [
        bigquery.SchemaField("top_overlap", bigquery.enums.SqlTypeNames.INT64),
        bigquery.SchemaField("level2_overlap", bigquery.enums.SqlTypeNames.INT64),
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
