import os
from typing import Dict, List
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import logging

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

logger = logging.getLogger("inspect_parquet")
logger.setLevel("INFO")


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
        pipeline_args[
            "subnetwork"
        ] = f"regions/{_DEFAULT_REGION}/subnetworks/{subnetwork}"

    return PipelineOptions(**pipeline_args)


filepath = "gs://etldata-prod-search-ranking-data-hkwv8r/feature_logging_training_data_parquet/query_pipeline_web_organic/tight_purchase/_DATE=2024-01-07/results/part-00000*"

run_on_dataflow = False
if run_on_dataflow:
    pipeline_options = create_beam_pipeline_dataflow_options(
        job_name="for-orson",
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


def check_num_listings(request: Dict):
    """Check the number of listings in a request dictionary of batched feature values."""
    # requestUUID is usually guaranteed to exist
    requestUUIDs = request["requestUUID"]
    batch_size = len(requestUUIDs)
    if batch_size < 48:
        logger.info(f"Batch size is {batch_size}")


with beam.Pipeline(options=pipeline_options) as pipeline:
    grouped_flattened_listings = (
        pipeline
        | "CreateFileList" >> beam.Create([filepath])
        | "ReadFiles" >> beam.io.ReadAllFromParquet()
        | "CheckNumListings" >> beam.Map(check_num_listings)
    )
