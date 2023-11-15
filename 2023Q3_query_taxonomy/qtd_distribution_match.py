import logging
import time
import argparse
import numpy as np
import json
from collections import Counter
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.internal.clients.bigquery import TableSchema
from apache_beam.io.gcp.internal.clients.bigquery import TableFieldSchema
from google.cloud import bigquery


class DistribMatchAnalysis(beam.DoFn):
    def _process_single_feature(self, x, is_count=False):
        """QTD features are in the format {"list": [{"element": "val1"}, {"element": "val2"}, ...]}
        here we process features to normal list, and 0-1 standardize purchase counts
        """
        x_out = [y["element"] for y in x["list"]]
        if is_count:
            count_sum = np.sum(x_out).astype(np.float32)
            if count_sum > 0:
                x_out = [y / count_sum for y in x_out]
            else:
                logging.warning(x["list"])
                x_out = [float(y) for y in x_out]
        return x_out

    def _process_query_taxonomy_distribution(self, paths, counts):
        """Make QTD distribution in the form of {path: score} histogram
        score sum to 1
        """
        paths = self._process_single_feature(paths)
        counts = self._process_single_feature(counts, is_count=True)
        out_counter = {paths[i]: counts[i] for i in range(len(paths))}
        return out_counter

    def _process_listing_distribution(self, listing_taxo_list):
        """Make listing taxonomy distribution in the form of {path: score} histogram
        score sum to 1
        """
        taxo_counter = Counter(listing_taxo_list)
        count_sum = np.sum([v for v in taxo_counter.values()]).astype(np.float32)
        if count_sum > 0:
            out_counter = {k: (v / count_sum) for k, v in taxo_counter.items()}
        else:
            logging.warning(taxo_counter)
            out_counter = {k: float(v) for k, v in taxo_counter.items()}
        return out_counter

    def _compute_distance(self, query_taxo_dict, listing_dict):
        """Compute distribution distance in the form of histogram distance"""
        distrib_dist = 0.0
        query_keys = set(query_taxo_dict.keys())
        listing_keys = set(listing_dict.keys())
        shared_keys = list(query_keys.intersection(listing_keys))
        query_only_key = list(query_keys - listing_keys)
        listing_only_key = list(listing_keys - query_keys)
        for k in shared_keys:
            distrib_dist += np.abs(query_taxo_dict[k] - listing_dict[k])
        for k in query_only_key:
            distrib_dist += query_taxo_dict[k]
        for k in listing_only_key:
            distrib_dist += listing_dict[k]
        return distrib_dist

    def process(self, row):
        # request level data
        out_data = {"uuid": row[0][0], "behavior": row[0][1]}
        grouped_data = row[1]
        out_data["query"] = grouped_data[0]["query"]
        out_data["query_bin"] = grouped_data[0]["query_bin"]
        out_data["query_intent"] = grouped_data[0]["query_intent"]

        # get query taxonomy paths & counts
        ppaths = grouped_data[0]["ppaths"]
        pcounts = grouped_data[0]["pcounts"]

        # get listing taxonomy from the page
        listing_taxonomy_list = []
        for item in grouped_data:
            if item["listing_taxo_level2"] is not None:
                listing_taxonomy_list.append(item["listing_taxo_level2"])

        # compute distance
        if (
            ppaths is not None
            and pcounts is not None
            and len(listing_taxonomy_list) >= 10
        ):
            if len(ppaths["list"]) > 0 and len(pcounts["list"]) > 0:
                # if we have enough values in both query taxo and listing taxo, compute distance
                qtd_distribution = self._process_query_taxonomy_distribution(
                    paths=ppaths,
                    counts=pcounts,
                )
                listing_distribution = self._process_listing_distribution(
                    listing_taxo_list=listing_taxonomy_list
                )
                request_distrib_dist = self._compute_distance(
                    qtd_distribution, listing_distribution
                )
                out_data["qtd_distrib"] = json.dumps(qtd_distribution)
                out_data["listing_taxo_distrib"] = json.dumps(listing_distribution)
                out_data["distrib_distance"] = request_distrib_dist

        # if computing distance not successful because some data is not available,
        # set as None and return
        if "qtd_distrib" not in out_data:
            out_data["qtd_distrib"] = None
            out_data["listing_taxo_distrib"] = None
            out_data["distrib_distance"] = None
        return [out_data]


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_table",
        required=True,
        help="Input table",
    )
    parser.add_argument(
        "--output_table",
        required=True,
        help="Output table with processed data",
    )
    args, _ = parser.parse_known_args(argv)

    now = str(int(time.time()))
    pipeline_options = PipelineOptions(
        save_main_session=True,
        pipeline_type_check=True,
        job_name=f"yzhang-last-pass-boost-{now}",
    )

    client = bigquery.Client(project=pipeline_options.get_all_options()["project"])
    output_schema_bq = [
        bigquery.SchemaField("uuid", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("behavior", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("query", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("query_bin", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("query_intent", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("qtd_distrib", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField(
            "listing_taxo_distrib", bigquery.enums.SqlTypeNames.STRING
        ),
        bigquery.SchemaField("distrib_distance", bigquery.enums.SqlTypeNames.FLOAT64),
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
            | "Read input data"
            >> beam.io.ReadFromBigQuery(
                query=f"select * from `{args.input_table}`",
                # query="select * from `etsy-sr-etl-prod.yzhang.qtd_distrib_match_qtdlevel2_raw` limit 200000",
                use_standard_sql=True,
                gcs_location=f"gs://etldata-prod-search-ranking-data-hkwv8r/data/shared/tmp",
            )
            | "Group by request" >> beam.GroupBy(lambda x: (x["uuid"], x["behavior"]))
            # | "Print" >> beam.Map(lambda x: logging.info(x))
            | "Compute distribution distance" >> beam.ParDo(DistribMatchAnalysis())
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


## Example row after GroupBy
# row = (
#     ("6f162ce9-c123-4a7b-a105-edd44c2b487a", "variant"),
#     [
#         {
#             "uuid": "6f162ce9-c123-4a7b-a105-edd44c2b487a",
#             "query": "victorian ring",
#             "listing_id": 1599846489,
#             "position": 34,
#             "behavior": "variant",
#             "query_bin": "top.01",
#             "query_intent": "Direct",
#             "ppaths": {"list": [{"element": "jewelry.rings"}]},
#             "pcounts": {"list": [{"element": 1}]},
#             "listing_taxo_level2": "jewelry.rings",
#         },
#         {
#             "uuid": "6f162ce9-c123-4a7b-a105-edd44c2b487a",
#             "query": "victorian ring",
#             "listing_id": 1577777311,
#             "position": 42,
#             "behavior": "variant",
#             "query_bin": "top.01",
#             "query_intent": "Direct",
#             "ppaths": {"list": [{"element": "jewelry.rings"}]},
#             "pcounts": {"list": [{"element": 1}]},
#             "listing_taxo_level2": "jewelry.rings",
#         },
#         {
#             "uuid": "6f162ce9-c123-4a7b-a105-edd44c2b487a",
#             "query": "victorian ring",
#             "listing_id": 1208173040,
#             "position": 40,
#             "behavior": "variant",
#             "query_bin": "top.01",
#             "query_intent": "Direct",
#             "ppaths": {"list": [{"element": "jewelry.rings"}]},
#             "pcounts": {"list": [{"element": 1}]},
#             "listing_taxo_level2": "jewelry.rings",
#         },
#         {
#             "uuid": "6f162ce9-c123-4a7b-a105-edd44c2b487a",
#             "query": "victorian ring",
#             "listing_id": 683768648,
#             "position": 25,
#             "behavior": "variant",
#             "query_bin": "top.01",
#             "query_intent": "Direct",
#             "ppaths": {"list": [{"element": "jewelry.rings"}]},
#             "pcounts": {"list": [{"element": 1}]},
#             "listing_taxo_level2": "jewelry.rings",
#         },
#     ],
# )
