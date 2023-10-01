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

# each row after groupby
row = (
    "00001f12-1b43-4b6f-b509-4b0799393f6b",
    [
        {
            "mmxRequestUUID": "00001f12-1b43-4b6f-b509-4b0799393f6b",
            "query": "natural shampoo bar",
            "userId": 0,
            "page_no": 1,
            "listingId": 1167646843,
            "position": 0,
            "listing_taxo_full_path": "bath_and_beauty.soaps.bar_soaps",
            "click_top_paths": {"list": [{"element": "bath_and_beauty"}]},
            "click_top_counts": {"list": [{"element": 117}]},
            "click_level2_paths": {
                "list": [
                    {"element": "bath_and_beauty.hair_care"},
                    {"element": "bath_and_beauty.soaps"},
                    {"element": "bath_and_beauty.essential_oils"},
                ]
            },
            "click_level2_counts": {
                "list": [{"element": 97}, {"element": 19}, {"element": 1}]
            },
            "purchase_top_paths": {"list": [{"element": "bath_and_beauty"}]},
            "purchase_top_counts": {"list": [{"element": 7}]},
            "purchase_level2_paths": {
                "list": [{"element": "bath_and_beauty.hair_care"}]
            },
            "purchase_level2_counts": {"list": [{"element": 7}]},
            "clicked": 0,
            "purchased": 0,
            "query_bin": "top.1",
            "buyer_segment": "Signed Out",
        },
        {
            "mmxRequestUUID": "00001f12-1b43-4b6f-b509-4b0799393f6b",
            "query": "natural shampoo bar",
            "userId": 0,
            "page_no": 1,
            "listingId": 846431563,
            "position": 1,
            "listing_taxo_full_path": "bath_and_beauty.soaps.bar_soaps",
            "click_top_paths": {"list": [{"element": "bath_and_beauty"}]},
            "click_top_counts": {"list": [{"element": 117}]},
            "click_level2_paths": {
                "list": [
                    {"element": "bath_and_beauty.hair_care"},
                    {"element": "bath_and_beauty.soaps"},
                    {"element": "bath_and_beauty.essential_oils"},
                ]
            },
            "click_level2_counts": {
                "list": [{"element": 97}, {"element": 19}, {"element": 1}]
            },
            "purchase_top_paths": {"list": [{"element": "bath_and_beauty"}]},
            "purchase_top_counts": {"list": [{"element": 7}]},
            "purchase_level2_paths": {
                "list": [{"element": "bath_and_beauty.hair_care"}]
            },
            "purchase_level2_counts": {"list": [{"element": 7}]},
            "clicked": 0,
            "purchased": 0,
            "query_bin": "top.1",
            "buyer_segment": "Signed Out",
        },
        {
            "mmxRequestUUID": "00001f12-1b43-4b6f-b509-4b0799393f6b",
            "query": "natural shampoo bar",
            "userId": 0,
            "page_no": 1,
            "listingId": 735212790,
            "position": 2,
            "listing_taxo_full_path": "bath_and_beauty.hair_care.shampoos",
            "click_top_paths": {"list": [{"element": "bath_and_beauty"}]},
            "click_top_counts": {"list": [{"element": 117}]},
            "click_level2_paths": {
                "list": [
                    {"element": "bath_and_beauty.hair_care"},
                    {"element": "bath_and_beauty.soaps"},
                    {"element": "bath_and_beauty.essential_oils"},
                ]
            },
            "click_level2_counts": {
                "list": [{"element": 97}, {"element": 19}, {"element": 1}]
            },
            "purchase_top_paths": {"list": [{"element": "bath_and_beauty"}]},
            "purchase_top_counts": {"list": [{"element": 7}]},
            "purchase_level2_paths": {
                "list": [{"element": "bath_and_beauty.hair_care"}]
            },
            "purchase_level2_counts": {"list": [{"element": 7}]},
            "clicked": 0,
            "purchased": 0,
            "query_bin": "top.1",
            "buyer_segment": "Signed Out",
        },
    ],
)


class ComputePageDistance(beam.DoFn):
    def _process_single_feature(self, x, is_count=False):
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
        paths = self._process_single_feature(paths)
        counts = self._process_single_feature(counts, is_count=True)
        out_counter = {paths[i]: counts[i] for i in range(len(paths))}
        return out_counter

    def _process_listing_distribution(self, taxo_counter):
        count_sum = np.sum([v for v in taxo_counter.values()]).astype(np.float32)
        if count_sum > 0:
            out_counter = {k: (v / count_sum) for k, v in taxo_counter.items()}
        else:
            logging.warning(taxo_counter)
            out_counter = {k: float(v) for k, v in taxo_counter.items()}
        return out_counter

    def _compute_distance(self, query_taxo_dict, listing_dict):
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
        row_data = row[1]
        out_data = {
            "mmxRequestUUID": row[0],
            "query": row_data[0]["query"],
            "page_no": row_data[0]["page_no"],
            "clicked": 0,
            "purchased": 0,
            "query_bin": row_data[0]["query_bin"],
            "buyer_segment": row_data[0]["buyer_segment"],
        }
        # collect listing taxonomy
        listing_top_taxo = []
        listing_level2_taxo = []
        for listing_data in row_data:
            listing_full_taxo = listing_data["listing_taxo_full_path"]
            listing_full_taxo_split = listing_full_taxo.split(".")
            if len(listing_full_taxo_split) > 1:
                listing_top_taxo.append(listing_full_taxo_split[0])
                listing_level2_taxo.append(
                    listing_full_taxo_split[0] + "." + listing_full_taxo_split[1]
                )
            else:
                listing_top_taxo.append(listing_full_taxo)
                listing_level2_taxo.append("")
            out_data["clicked"] += listing_data["clicked"]
            out_data["purchased"] += listing_data["purchased"]
        listing_top_taxo_counter = Counter(listing_top_taxo)
        listing_top_distrib = self._process_listing_distribution(
            listing_top_taxo_counter
        )
        listing_level2_taxo_counter = Counter(listing_level2_taxo)
        listing_level2_distrib = self._process_listing_distribution(
            listing_level2_taxo_counter
        )
        # clean up query taxo features
        click_top_distrib = self._process_query_taxonomy_distribution(
            paths=row_data[0]["click_top_paths"],
            counts=row_data[0]["click_top_counts"],
        )
        purchase_top_distrib = self._process_query_taxonomy_distribution(
            paths=row_data[0]["purchase_top_paths"],
            counts=row_data[0]["purchase_top_counts"],
        )
        click_level2_distrib = self._process_query_taxonomy_distribution(
            paths=row_data[0]["click_level2_paths"],
            counts=row_data[0]["click_level2_counts"],
        )
        purchase_level2_distrib = self._process_query_taxonomy_distribution(
            paths=row_data[0]["purchase_level2_paths"],
            counts=row_data[0]["purchase_level2_counts"],
        )
        # compute distance
        out_data["dist_click_top"] = self._compute_distance(
            click_top_distrib, listing_top_distrib
        )
        out_data["dist_purchase_top"] = self._compute_distance(
            purchase_top_distrib, listing_top_distrib
        )
        out_data["dist_click_level2"] = self._compute_distance(
            click_level2_distrib, listing_level2_distrib
        )
        out_data["dist_purchase_level2"] = self._compute_distance(
            purchase_level2_distrib, listing_level2_distrib
        )
        # record distributions
        out_data["listing_top_distrib"] = json.dumps(listing_top_distrib)
        out_data["click_top_distrib"] = json.dumps(click_top_distrib)
        out_data["purchase_top_distrib"] = json.dumps(purchase_top_distrib)
        out_data["listing_level2_distrib"] = json.dumps(listing_level2_distrib)
        out_data["click_level2_distrib"] = json.dumps(click_level2_distrib)
        out_data["purchase_level2_distrib"] = json.dumps(purchase_level2_distrib)
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
        job_name=f"yzhang-distrib-match-opp-sizing-{now}",
    )

    client = bigquery.Client(project=pipeline_options.get_all_options()["project"])
    output_schema_bq = [
        bigquery.SchemaField("mmxRequestUUID", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("query", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("page_no", bigquery.enums.SqlTypeNames.INT64),
        bigquery.SchemaField("clicked", bigquery.enums.SqlTypeNames.INT64),
        bigquery.SchemaField("purchased", bigquery.enums.SqlTypeNames.INT64),
        bigquery.SchemaField("query_bin", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("buyer_segment", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("dist_click_top", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("dist_purchase_top", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("dist_click_level2", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField(
            "dist_purchase_level2", bigquery.enums.SqlTypeNames.FLOAT64
        ),
        bigquery.SchemaField("listing_top_distrib", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("click_top_distrib", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField(
            "purchase_top_distrib", bigquery.enums.SqlTypeNames.STRING
        ),
        bigquery.SchemaField(
            "listing_level2_distrib", bigquery.enums.SqlTypeNames.STRING
        ),
        bigquery.SchemaField(
            "click_level2_distrib", bigquery.enums.SqlTypeNames.STRING
        ),
        bigquery.SchemaField(
            "purchase_level2_distrib", bigquery.enums.SqlTypeNames.STRING
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
                query=f"select * from `{args.input_table}` order by mmxRequestUUID, position limit 1000",
                # query="select * from `etsy-sr-etl-prod.yzhang.query_taxo_web_full` limit 100",
                use_standard_sql=True,
                gcs_location=f"gs://etldata-prod-search-ranking-data-hkwv8r/data/shared/tmp",
            )
            | "Group by request" >> beam.GroupBy(lambda x: x["mmxRequestUUID"])
            # | "Print" >> beam.Map(lambda x: logging.warning(x))
            | "Compute distribution distance" >> beam.ParDo(ComputePageDistance())
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
