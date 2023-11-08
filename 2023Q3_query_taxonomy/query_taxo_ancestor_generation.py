import logging
import time
import argparse
import numpy as np
import datetime
import copy
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.internal.clients.bigquery import TableSchema
from apache_beam.io.gcp.internal.clients.bigquery import TableFieldSchema
from google.cloud import bigquery

row = {
    "query": "custom dog patch velcro",
    "level2_path_raw": {
        "list": [{"element": "pet_supplies.pet_clothing_accessories_and_shoes"}]
    },
    "level2_path": {"element": "pet_supplies.pet_clothing_accessories_and_shoes"},
}


class QueryTaxoAncestorGen(beam.DoFn):
    def process(self, row):
        paths_raw = [item["element"] for item in row["level2_path_raw"]["list"]]
        curr_path = row["level2_path"]["element"]
        out_data = {
            "query": row["query"],
            "level2_path": curr_path,
        }
        curr_idx = paths_raw.index(curr_path)
        if curr_idx == 0:
            out_data["level2_ancestor"] = None
        else:
            out_data["level2_ancestor"] = paths_raw[curr_idx - 1]
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
        job_name=f"yzhang-last-pass-boost-parameter-{now}",
    )

    client = bigquery.Client(project=pipeline_options.get_all_options()["project"])
    output_schema_bq = [
        bigquery.SchemaField("query", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("level2_path", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("level2_ancestor", bigquery.enums.SqlTypeNames.STRING),
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
                # query="select * from `etsy-sr-etl-prod.yzhang.query_taxo_lastpass_rpc_taxo_ancenstor_raw` limit 100",
                use_standard_sql=True,
                gcs_location=f"gs://etldata-prod-search-ranking-data-hkwv8r/data/shared/tmp",
            )
            # | "Print" >> beam.Map(lambda x: logging.info(x))
            | "Query taxo listing process" >> beam.ParDo(QueryTaxoAncestorGen())
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
