import logging
import time
import argparse
import numpy as np
import datetime
import copy
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.internal.clients.bigquery import TableSchema, TableFieldSchema
from google.cloud import bigquery

DISTRIB_THRESHOLD = np.arange(0, 1.05, 0.05)
DISTRIB_THRESHOLD_LABEL = [str(int(x * 100)) for x in DISTRIB_THRESHOLD]

# row = {
#     "mmxRequestUUID": "4c063d81-41ea-4717-9c62-4c4a1aa22198",
#     "query": "shopify website template beauty boutique asthetic",
#     "userId": 846323386,
#     "page_no": 1,
#     "listingId": 1202070399,
#     "position": 42,
#     "query_date": datetime.date(2023, 10, 17),
#     "query_bin": None,
#     "paths": [
#         "paper_and_party_supplies.paper.stationery.design_and_templates.templates.planner_templates",
#         "paper_and_party_supplies.paper.calendars_and_planners",
#         "books_movies_and_music.books.blank_books.journals_and_notebooks",
#     ],
#     "predicted_prob": [0.9664, 0.0198, 0.0065],
#     "buyer_segment": None,
#     "full_path": "paper_and_party_supplies.paper.stationery.design_and_templates.templates",
#     "winsorized_gms": None,
# }
# query_taxo_paths = [
#     "paper_and_party_supplies.paper.stationery.design_and_templates.templates.planner_templates",
#     "paper_and_party_supplies.paper.calendars_and_planners",
#     "books_movies_and_music.books.blank_books.journals_and_notebooks",
# ]
# query_taxo_prob = [0.9664, 0.0198, 0.0065]


class QueryTaxoBertListingProcess(beam.DoFn):
    def __init__(self, norm_prob=False):
        self.norm_prob = norm_prob

    def process(self, row):
        out_data = copy.deepcopy(row)
        if len(row["paths"]) > 0:
            query_taxo_paths, query_taxo_prob = row["paths"], row["predicted_prob"]
            # sort both paths and probs by descending probs
            path_score_sorted = sorted(
                list(zip(query_taxo_prob, query_taxo_paths)), reverse=True
            )
            query_taxo_prob = [item[0] for item in path_score_sorted]
            query_taxo_paths = [item[1] for item in path_score_sorted]
            # normalize score to sum to 1 if specified
            if self.norm_prob:
                distrib_sum = np.sum(query_taxo_prob)
                query_taxo_distrib = [x / distrib_sum for x in query_taxo_prob]
                query_taxo_prob = query_taxo_distrib

            listing_taxo = row["full_path"]
            ## remove listings if not overlap, or (overlaps and prob < threshold)
            if listing_taxo is not None:
                if listing_taxo in query_taxo_paths:
                    # listing overlaps with query taxo
                    idx = query_taxo_paths.index(listing_taxo)
                    prob_score = query_taxo_prob[idx]
                    if prob_score == 0:
                        out_data["th0"] = "remove"
                    else:
                        out_data["th0"] = "keep"
                    # threshold above 0
                    for i in range(1, len(DISTRIB_THRESHOLD)):
                        if prob_score < DISTRIB_THRESHOLD[i]:
                            out_data[f"th{DISTRIB_THRESHOLD_LABEL[i]}"] = "remove"
                        else:
                            out_data[f"th{DISTRIB_THRESHOLD_LABEL[i]}"] = "keep"
                else:
                    # listing does not overlap with query taxo
                    for i in range(len(DISTRIB_THRESHOLD)):
                        out_data[f"th{DISTRIB_THRESHOLD_LABEL[i]}"] = "remove"

        ## if missing query or listing taxo info, default keep listing
        for th in DISTRIB_THRESHOLD_LABEL:
            if f"th{th}" not in out_data:
                out_data[f"th{th}"] = "keep"

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

    output_schema_bq = copy.deepcopy(in_schema_bq)
    for th in DISTRIB_THRESHOLD_LABEL:
        output_schema_bq.append(
            bigquery.SchemaField(f"th{th}", bigquery.enums.SqlTypeNames.STRING)
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
                # query="select * from `etsy-sr-etl-prod.yzhang.query_taxo_bert_lastpass_rpc` where array_length(paths) > 0 limit 100",
                use_standard_sql=True,
                gcs_location=f"gs://etldata-prod-search-ranking-data-hkwv8r/data/shared/tmp",
            )
            | "Query taxo listing process"
            >> beam.ParDo(QueryTaxoBertListingProcess(norm_prob=True))
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
