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

DISTRIB_THRESHOLD = np.arange(0, 0.55, 0.05)
DISTRIB_THRESHOLD_LABEL = [str(int(x * 100)) for x in DISTRIB_THRESHOLD]
PURCHASE_COUNT_THRESHOLD = 0

row = {
    "mmxRequestUUID": "6ff08a13-87fc-49b7-bde3-4b9dd7166fc2",
    "query": "tattoo font",
    "userId": 16585734,
    "page_no": 1,
    "listingId": 1482660131,
    "position": 7,
    "query_date": datetime.date(2023, 9, 30),
    "query_bin": "top.1",
    "buyer_segment": "Habitual",
    "purchase_top_paths": {
        "list": [
            {"element": "art_and_collectibles"},
            {"element": "craft_supplies_and_tools"},
        ]
    },
    "purchase_top_counts": {"list": [{"element": 60}, {"element": 1}]},
    "purchase_level2_paths": {
        "list": [
            {"element": "art_and_collectibles.drawing_and_illustration"},
            {"element": "art_and_collectibles.prints"},
        ]
    },
    "purchase_level2_counts": {"list": [{"element": 57}, {"element": 3}]},
    "listing_top_taxo": "art_and_collectibles",
    "listing_second_taxo": "art_and_collectibles.drawing_and_illustration",
    "winsorized_gms": "2146.09",
}
purchase_top_paths = ["art_and_collectibles", "craft_supplies_and_tools"]
purchase_top_distrib = [60.0 / 61.0, 1.0 / 61.0]
purchase_level2_paths = [
    "art_and_collectibles.prints",
    "art_and_collectibles.drawing_and_illustration",
]
purchase_level2_distrib = [57.0 / 60.0, 3.0 / 60.0]


class QueryTaxoListingProcess(beam.DoFn):
    def _process_single_feature(self, x, is_count=False):
        x_out = [y["element"] for y in x["list"]]
        if is_count:
            x_out = [y if y >= PURCHASE_COUNT_THRESHOLD else 0 for y in x_out]
            count_sum = np.sum(x_out).astype(np.float32)
            if count_sum > 0:
                x_out = [y / count_sum for y in x_out]
            else:
                logging.warning(x["list"])
                x_out = [float(y) for y in x_out]
        return x_out

    def process(self, row):
        out_data = {
            k: v
            for k, v in row.items()
            if k
            not in [
                "purchase_top_paths",
                "purchase_top_counts",
                "purchase_level2_paths",
                "purchase_level2_counts",
            ]
        }
        if (
            row["purchase_top_paths"] is not None
            and row["purchase_level2_paths"] is not None
        ):
            purchase_top_paths = self._process_single_feature(row["purchase_top_paths"])
            purchase_top_distrib = self._process_single_feature(
                row["purchase_top_counts"], is_count=True
            )
            purchase_level2_paths = self._process_single_feature(
                row["purchase_level2_paths"]
            )
            purchase_level2_distrib = self._process_single_feature(
                row["purchase_level2_counts"], is_count=True
            )
            listing_top_taxo = row["listing_top_taxo"]
            listing_level2_taxo = row["listing_second_taxo"]
            ## remove listings if not overlap with purchased taxonomy
            ## remove listings if overlaps and puchase likelihood < threshold
            ## top taxo
            if listing_top_taxo is not None:
                if listing_top_taxo in purchase_top_paths:
                    out_data["top0"] = "keep"
                    idx = purchase_top_paths.index(listing_top_taxo)
                    ptop = purchase_top_distrib[idx]
                    for i in range(1, len(DISTRIB_THRESHOLD)):
                        if ptop < DISTRIB_THRESHOLD[i]:
                            out_data[f"top{DISTRIB_THRESHOLD_LABEL[i]}"] = "remove"
                        else:
                            out_data[f"top{DISTRIB_THRESHOLD_LABEL[i]}"] = "keep"
                else:
                    for i in range(len(DISTRIB_THRESHOLD)):
                        out_data[f"top{DISTRIB_THRESHOLD_LABEL[i]}"] = "remove"
            ## level2 taxo
            if listing_level2_taxo is not None:
                if listing_level2_taxo in purchase_level2_paths:
                    out_data["second0"] = "keep"
                    idx = purchase_level2_paths.index(listing_level2_taxo)
                    plevel2 = purchase_level2_distrib[idx]
                    for i in range(1, len(DISTRIB_THRESHOLD)):
                        if plevel2 < DISTRIB_THRESHOLD[i]:
                            out_data[f"second{DISTRIB_THRESHOLD_LABEL[i]}"] = "remove"
                        else:
                            out_data[f"second{DISTRIB_THRESHOLD_LABEL[i]}"] = "keep"
                else:
                    for i in range(len(DISTRIB_THRESHOLD)):
                        out_data[f"second{DISTRIB_THRESHOLD_LABEL[i]}"] = "remove"
        ## if missing query or listing taxo info, default keep listing
        for th in DISTRIB_THRESHOLD_LABEL:
            if f"top{th}" not in out_data:
                out_data[f"top{th}"] = "keep"
        for th in DISTRIB_THRESHOLD_LABEL:
            if f"second{th}" not in out_data:
                out_data[f"second{th}"] = "keep"
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
    in_table = client.get_table(args.input_table)
    in_schema_bq = in_table.schema
    in_schema_bq_reduced = [
        item
        for item in in_schema_bq
        if item.name
        not in [
            "purchase_top_paths",
            "purchase_top_counts",
            "purchase_level2_paths",
            "purchase_level2_counts",
        ]
    ]
    output_schema_bq = copy.deepcopy(in_schema_bq_reduced)
    for th in DISTRIB_THRESHOLD_LABEL:
        output_schema_bq.append(
            bigquery.SchemaField(f"top{th}", bigquery.enums.SqlTypeNames.STRING)
        )
    for th in DISTRIB_THRESHOLD_LABEL:
        output_schema_bq.append(
            bigquery.SchemaField(f"second{th}", bigquery.enums.SqlTypeNames.STRING)
        )
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
                # query="select * from `etsy-sr-etl-prod.yzhang.query_taxo_lastpass_rpc` limit 100",
                use_standard_sql=True,
                gcs_location=f"gs://etldata-prod-search-ranking-data-hkwv8r/data/shared/tmp",
            )
            # | "Print" >> beam.Map(lambda x: logging.info(x))
            | "Query taxo listing process" >> beam.ParDo(QueryTaxoListingProcess())
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
