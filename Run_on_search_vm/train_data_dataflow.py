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
FILEPATH = "gs://etldata-prod-search-ranking-data-hkwv8r/user/yzhang/listing_signals_v5/feature_logging_training_data_parquet/query_pipeline_web_organic/tight_purchase/_DATE=2024-03-05/results/part-*.parquet"
RUN_ON_DATAFLOW = True
OUTPUT_TABLE = "etsy-sr-etl-prod:yzhang.lsig_v5_web_tight_0305"
JOB_NAME = "yz-lsig-web-tight-0305"
####

_DEFAULT_PROJECT = "etsy-search-ml-dev"
_DEFAULT_REGION = "us-central1"
_DEFAULT_NUM_WORKERS = 4
_DEFAULT_MAX_NUM_WORKERS = 64
_DEFAULT_MACHINE_TYPE = "c2d-highmem-8"
_DEFAULT_BUCKET = "gs://training-dev-search-data-jtzn/neural_ranking/second_pass"
_DEFAULT_VPC = "project-vpc"
_DEFAULT_AUTOSCALING_ALGORITHM = "THROUGHPUT_BASED"
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
    use_public_ips: bool = False,
    service_account: str = "sa-dataflow@etsy-search-ml-dev.iam.gserviceaccount.com",
    sdk_container_image: str = None,
    sdk_location: str = "container",
    environment_type: str = None,
    ttl: int = 28800,  # 12 hours
    experiments: List = [
        "use_avro",
        "use_runner_v2",
        "upload_graph",
    ],
    subnetwork: str = _DEFAULT_VPC,
    autoscaling_algorithm: str = _DEFAULT_AUTOSCALING_ALGORITHM,
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
        "use_public_ips": use_public_ips,
        "service_account_email": service_account,
        "environment_type": environment_type,
        "sdk_container_image": sdk_container_image,
        "sdk_location": sdk_location,
        "experiments": experiments,
        "autoscaling_algorithm": autoscaling_algorithm,
        "disk_size_gb": disk_size_gb,
        "number_of_worker_harness_threads": number_of_worker_harness_threads,
    }

    if not use_public_ips:
        pipeline_args["subnetwork"] = (
            f"regions/{_DEFAULT_REGION}/subnetworks/{subnetwork}"
        )

    return PipelineOptions(**pipeline_args)


class ListingSignalDataProcess(beam.DoFn):
    def process(self, request):
        try:
            logging.info(f"{len(request['requestUUID'])=}")
            requestUUIDs = request["requestUUID"]
            listing_ids = request["candidateInfo.docInfo.listingInfo.listingId"]
            if listing_ids is None:
                listing_ids = request["candidateInfo.candidateId"]

            for i in range(len(requestUUIDs)):
                out_data = {
                    "requestUUID": requestUUIDs[i],
                    "listing_id": listing_ids[i],
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
                return [out_data]
        except:
            logging.info(f"Failed to read in: {np.unique(request['requestUUID'])}")
            logging.info(f"{len(request['requestUUID'])=}")
            logging.info(f"{request['candidateInfo.docInfo.listingInfo.listingId']=}")
            logging.info(f"{request['candidateInfo.candidateId']=}")


def run(argv=None):
    # parser = argparse.ArgumentParser()
    # parser.add_argument(
    #     "--input_path",
    #     required=False,
    # )
    # parser.add_argument(
    #     "--output_table",
    #     required=False,
    # )
    # parser.add_argument(
    #     "--job_name",
    #     required=False,
    # )
    # args, _ = parser.parse_known_args(argv)

    if RUN_ON_DATAFLOW:
        pipeline_options = create_beam_pipeline_dataflow_options(
            job_name=JOB_NAME,
            num_workers=10,
            max_num_workers=500,
            ttl=10800,  # 3 hours
            save_main_session=False,
            environment_type="DOCKER",
            autoscaling_algorithm="NONE",
            machine_type="c2d-highmem-8",
            number_of_worker_harness_threads=None,
            sdk_container_image="gcr.io/etsy-gcr-dev/ml-infra/vertex/pipelines/neural_ranking/second_pass:latest",
        )
    else:
        pipeline_options = None

    client = bigquery.Client(project=pipeline_options.get_all_options()["project"])
    output_schema_bq = [
        bigquery.SchemaField("requestUUID", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("listing_id", bigquery.enums.SqlTypeNames.INT64),
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
