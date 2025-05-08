import logging
import os
from typing import List
import numpy as np
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.internal.clients.bigquery import TableSchema
from apache_beam.io.gcp.internal.clients.bigquery import TableFieldSchema
from google.cloud import bigquery

####  Parameters to change
FILEPATH = "gs://etldata-prod-search-ranking-data-hkwv8r/user/yzhang/listing_signals_v5/feature_logging_training_data_parquet/query_pipeline_boe_organic/tight_purchase/_DATE=2024-02-26/results/part-*.parquet"
OUTPUT_TABLE = "etsy-sr-etl-prod:yzhang.lsig_v5_boe_tight_0226"
JOB_NAME = "yz-lsig-boe-tight-0226"
RUN_ON_DATAFLOW = True
####

_DEFAULT_PROJECT = "etsy-sr-etl-prod"
_DEFAULT_REGION = "us-central1"
_DEFAULT_NUM_WORKERS = 4
_DEFAULT_MAX_NUM_WORKERS = 64
_DEFAULT_MACHINE_TYPE = "c2d-highmem-8"
_DEFAULT_BUCKET = "gs://etldata-prod-search-ranking-data-hkwv8r/data/shared/tmp"
_DEFAULT_DISK_SIZE_GB = 400
_DEFAULT_NUMBER_OF_WORKER_HARNESS_THREADS = None


def create_beam_pipeline_dataflow_options(
    job_name,
    num_workers: int = _DEFAULT_NUM_WORKERS,
    max_num_workers: int = _DEFAULT_MAX_NUM_WORKERS,
    machine_type: str = _DEFAULT_MACHINE_TYPE,
    save_main_session: bool = True,
    project: str = _DEFAULT_PROJECT,
    bucket: str = _DEFAULT_BUCKET,
    service_account: str = "dataflow-worker@etsy-sr-etl-prod.iam.gserviceaccount.com",
    ttl: int = 28800,  # 12 hours
    experiments: List = [
        "use_avro",
        "use_runner_v2",
        "upload_graph",
    ],
    disk_size_gb: int = _DEFAULT_DISK_SIZE_GB,
    number_of_worker_harness_threads: int = _DEFAULT_NUMBER_OF_WORKER_HARNESS_THREADS,
) -> PipelineOptions:
    temp_location = os.path.join(bucket, "tmp", job_name)

    if num_workers <= 100:
        ttl = min(ttl, 28800)  # 12 hours
    else:
        ttl = min(ttl, 10800)  # 3 hours

    experiments.append(f"max_workflow_runtime_walltime_seconds={ttl}")

    pipeline_args = {
        "runner": "DataflowRunner",
        "project": project,
        "region": _DEFAULT_REGION,
        "job_name": job_name,
        "num_workers": num_workers,
        "max_num_workers": max_num_workers,
        "machine_type": machine_type,
        "save_main_session": save_main_session,
        "temp_location": temp_location,
        "service_account_email": service_account,
        "experiments": experiments,
        "disk_size_gb": disk_size_gb,
        "number_of_worker_harness_threads": number_of_worker_harness_threads,
    }

    return PipelineOptions(**pipeline_args)


class ListingSignalDataProcess(beam.DoFn):
    def process(self, request):
        try:
            logging.info(f"{len(request['requestUUID'])=}")
            requestUUIDs = request["requestUUID"]
            listing_ids = request["candidateInfo.docInfo.listingInfo.listingId"]
            client_queries = request["clientProvidedInfo.query.query"]
            context_queries = request[
                "contextualInfo[name=target].docInfo.queryInfo.query"
            ]
            query_is_digital = request[
                "contextualInfo[name=target].docInfo.queryInfo.queryLevelMetrics.isDigital"
            ]
            listing_is_digital = request[
                "candidateInfo.docInfo.listingInfo.listingWeb.isDigital"
            ]
            if listing_ids is None:
                listing_ids = request["candidateInfo.candidateId"]

            out_data_list = []
            for i in range(len(requestUUIDs)):
                out_data = {
                    "requestUUID": requestUUIDs[i],
                    "listing_id": listing_ids[i],
                    "attributions": (
                        1 if "purchase" in request["attributions"][i] else 0
                    ),
                    "client_queries": (
                        client_queries[i] if client_queries is not None else None
                    ),
                    "context_queries": (
                        context_queries[i] if context_queries is not None else None
                    ),
                    "query_is_digital": (
                        int(query_is_digital[i])
                        if query_is_digital is not None
                        else None
                    ),
                    "listing_is_digital": (
                        int(listing_is_digital[i])
                        if listing_is_digital is not None
                        else None
                    ),
                }

                # price
                listing_web_price_keys = request[
                    "candidateInfo.docInfo.listingInfo.listingWeb.price#keys"
                ][i]
                listing_web_price_values = request[
                    "candidateInfo.docInfo.listingInfo.listingWeb.price#values"
                ][i]
                listing_web_price = None
                if listing_web_price_keys is not None:
                    for j in range(len(listing_web_price_keys)):
                        if listing_web_price_keys[j] == "US":
                            listing_web_price = listing_web_price_values[j] / 100.0
                out_data["lw_price"] = listing_web_price

                # promo price
                promo_price_keys = request[
                    "candidateInfo.docInfo.listingInfo.listingWeb.promotionalPrice#keys"
                ][i]
                promo_price_values = request[
                    "candidateInfo.docInfo.listingInfo.listingWeb.promotionalPrice#values"
                ][i]
                promo_price = None
                if promo_price_keys is not None:
                    for j in range(len(promo_price_keys)):
                        if promo_price_keys[j] == "US":
                            promo_price = promo_price_values[j] / 100.0
                out_data["lw_promo_price"] = promo_price

                alb_price_feature = request[
                    "candidateInfo.docInfo.listingInfo.activeListingBasics.priceUsd"
                ]
                alb_min_price_feature = request[
                    "candidateInfo.docInfo.listingInfo.activeListingBasics.minPriceUsd"
                ]
                alb_max_price_feature = request[
                    "candidateInfo.docInfo.listingInfo.activeListingBasics.maxPriceUsd"
                ]

                out_data["alb_price"] = (
                    alb_price_feature[i] if alb_price_feature is not None else None
                )
                out_data["alb_min_price"] = (
                    alb_min_price_feature[i]
                    if alb_min_price_feature is not None
                    else None
                )
                out_data["alb_max_price"] = (
                    alb_max_price_feature[i]
                    if alb_max_price_feature is not None
                    else None
                )
                out_data_list.append(out_data)

            return out_data_list
        except:
            import numpy as np

            logging.info(f"Failed to read in: {np.unique(request['requestUUID'])}")
            logging.info(f"{len(request['requestUUID'])=}")
            logging.info(f"{request['candidateInfo.docInfo.listingInfo.listingId']=}")
            logging.info(f"{request['candidateInfo.candidateId']=}")


def run(argv=None):
    if RUN_ON_DATAFLOW:
        pipeline_options = create_beam_pipeline_dataflow_options(
            job_name=JOB_NAME,
            num_workers=10,
            max_num_workers=500,
            ttl=10800,  # 3 hours
            save_main_session=False,
            number_of_worker_harness_threads=None,
        )
    else:
        pipeline_options = None

    client = bigquery.Client(project=pipeline_options.get_all_options()["project"])
    output_schema_bq = [
        bigquery.SchemaField("requestUUID", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("listing_id", bigquery.enums.SqlTypeNames.INT64),
        bigquery.SchemaField("attributions", bigquery.enums.SqlTypeNames.INT64),
        bigquery.SchemaField("client_queries", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("context_queries", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("query_is_digital", bigquery.enums.SqlTypeNames.INT64),
        bigquery.SchemaField("listing_is_digital", bigquery.enums.SqlTypeNames.INT64),
        bigquery.SchemaField("lw_price", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("lw_promo_price", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("alb_price", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("alb_min_price", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("alb_max_price", bigquery.enums.SqlTypeNames.FLOAT64),
    ]
    out_table = bigquery.Table(OUTPUT_TABLE.replace(":", "."), schema=output_schema_bq)
    client.create_table(out_table, exists_ok=True)

    output_schema_beam = TableSchema()
    for item in output_schema_bq:
        field_schema = TableFieldSchema()
        field_schema.name = item.name
        field_schema.type = item.field_type
        field_schema.mode = item.mode
        output_schema_beam.fields.append(field_schema)

    with beam.Pipeline(options=pipeline_options) as pipeline:
        (
            pipeline
            | "CreateFileList" >> beam.Create([FILEPATH])
            | "ReadFiles" >> beam.io.ReadAllFromParquet()
            | "GetFields" >> beam.ParDo(ListingSignalDataProcess())
            # | "Print" >> beam.Map(print)
            | "Write results to BigQuery"
            >> beam.io.WriteToBigQuery(
                OUTPUT_TABLE,
                schema=output_schema_beam,
                method="FILE_LOADS",
            )
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
