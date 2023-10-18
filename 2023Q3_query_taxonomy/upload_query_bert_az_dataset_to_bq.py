import logging
import time
import argparse
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.internal.clients.bigquery import TableSchema
from apache_beam.io.gcp.internal.clients.bigquery import TableFieldSchema
from google.cloud import bigquery

# taxo_data = '[{"taxonomy": "jewelry.earrings.stud_earrings", "predicted_probability": 0.9365665912628174, "scaled_probability": 2.0}, {"taxonomy": "jewelry.earrings.screw_back_earrings", "predicted_probability": 0.02278164029121399, "scaled_probability": 1.0227261593978056}]'
# taxo_data = json.loads(taxo_data)


class ProcessQueryBert(beam.DoFn):
    def process(self, row):
        row_split = row.split("\t")
        query = row_split[0]
        out_data = {
            "query": query,
            "taxonomy": [],
            "predicted_probability": [],
            "scaled_probability": [],
        }
        taxo_data = json.loads(row_split[1])
        if len(taxo_data) > 0:
            for dt in taxo_data:
                out_data["taxonomy"].append(dt["taxonomy"])
                out_data["predicted_probability"].append(dt["predicted_probability"])
                out_data["scaled_probability"].append(dt["scaled_probability"])
        return [out_data]


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_path",
        required=True,
        help="Input data path",
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
        job_name=f"yzhang-upload-query-bert-{now}",
    )

    client = bigquery.Client(project=pipeline_options.get_all_options()["project"])
    output_schema_bq = [
        bigquery.SchemaField("query", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField(
            "taxonomy", bigquery.enums.SqlTypeNames.STRING, mode="REPEATED"
        ),
        bigquery.SchemaField(
            "predicted_probability",
            bigquery.enums.SqlTypeNames.FLOAT64,
            mode="REPEATED",
        ),
        bigquery.SchemaField(
            "scaled_probability", bigquery.enums.SqlTypeNames.FLOAT64, mode="REPEATED"
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
            | "Read input data" >> beam.io.ReadFromText(f"{args.input_path}/part-*")
            # | "Print" >> beam.Map(logging.info)
            | "Data formatting" >> beam.ParDo(ProcessQueryBert())
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
