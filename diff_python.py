import datetime
from airflow import DAG
from etsy.operators.gke import GKEDataflowPythonLaunch
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryExecuteQueryOperator,
)
from etsy.utils.defaults import patch_all

patch_all()

DEFAULT_TASK_ARGS = {
    "sla": datetime.timedelta(days=1),
    "owner": "search",
    "retries": 2,
    "retry_delay": datetime.timedelta(seconds=300),
    "start_date": datetime.datetime(2024, 8, 2, 0, 0),
    "dag_monitor_email": [
        "jgammack@etsy.com",
        "yzhang@etsy.com",
        "ebenjamin@etsy.com",
        "aclapp@etsy.com",
        "brussell@etsy.com",
        "search-ranking-team@etsy.pagerduty.com",  # pls ping @search-semantic-relevance-engs in #search-semantic-relevance
    ],
}

with DAG(
    dag_id="semantic_relevance_baseline",
    catchup=True,
    max_active_runs=4,
    max_active_tasks=4,
    schedule="@daily",
    default_args=DEFAULT_TASK_ARGS,
) as dag:
    sampling_table = "etsy-data-warehouse-prod.search.sem_rel_hydrated_daily_requests"

    inference_result_table_prefix = ""
    inference_output_pairs_table_name = (
        "etsy-data-warehouse-prod.search.sem_rel_query_listing_metrics"
    )
    inference_output_requests_table_name = (
        "etsy-data-warehouse-prod.search.sem_rel_requests_metrics"
    )
    # if inference_result_table_prefix is not ""
    # inference results will be written to
    # pairs table: [inference_result_table_prefix]_query_listing_metrics(_vw), and
    # request table: [inference_result_table_prefix]_request_metrics(_vw)
    # otherwise, it will use the provided table names

    ##  Sample requests and write data to BigQuery
    sample_search_requests_for_inference = BigQueryExecuteQueryOperator(
        task_id="sample_search_requests_for_inference",
        sql="""
        SELECT * FROM `etsy-data-warehouse-prod.search.sem_rel_get_requests`('{{ macros.etsy.hyphenated_days_ago(1, ds) }}', 88, 0.0015, 'web', 48, 16)
        UNION ALL
        SELECT * FROM `etsy-data-warehouse-prod.search.sem_rel_get_requests`('{{ macros.etsy.hyphenated_days_ago(1, ds) }}', 74, 0.0015, 'mweb', 34, 15)
        UNION ALL
        SELECT * FROM `etsy-data-warehouse-prod.search.sem_rel_get_requests`('{{ macros.etsy.hyphenated_days_ago(1, ds) }}', 68, 0.0015, 'boe', 28, 9)
        """,
        destination_dataset_table=sampling_table,
        write_disposition="WRITE_APPEND",
        create_disposition="CREATE_IF_NEEDED",
        time_partitioning={"field": "date", "type": "DAY"},
        use_legacy_sql=False,
        maximum_bytes_billed=36000000000000.0,
        location="US",
    )

    ##  Semantic relevance model inference
    timestamp = datetime.datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    job_name_prefix = "sem-rel-baseline-dashboard"
    project_id = "{{ macros.etsy.test_switch(in_test='etsy-search-ml-dev', in_prod='etsy-search-ml-prod') }}"
    SERVICE_ACCOUNT_NAME = "{{ macros.etsy.test_switch(in_test='airflow-search-aiplatform-dev', in_prod='airflow-search-aiplatform-prod') }}"

    model_args_dicts = [
        # V1
        {
            "model_type": "v1",
            "model_name": "bert-cern-l24-h1024-a16",
            "model_path": "gs://training-dev-search-data-jtzn/semantic_relevance/production_models/bert-cern-l24-h1024-a16-v1/export/saved_model",
            "tag": "v2.2.7",
            "num_workers": "10",
            "batch_size": "128",
        },
        # V2
        {
            "model_type": "v2",
            "model_name": "v2-deberta-v3-large-tad",
            "model_path": "gs://training-dev-search-data-jtzn/semantic_relevance/production_models/v2-deberta-v3-large-tad/export/saved_model",
            "tag": "v2.2.7",
            "num_workers": "32",
            "batch_size": "8",
        },
    ]

    for model_args in model_args_dicts:
        model_type = model_args["model_type"]
        model_name = model_args["model_name"]
        model_path = model_args["model_path"]
        tag = model_args["tag"]
        num_workers = model_args["num_workers"]
        batch_size = model_args["batch_size"]

        job_name = f"{job_name_prefix}-{model_name}"
        job_id = f"{job_name}-{timestamp}"

        image = (
            f"gcr.io/etsy-gcr-dev/ml-infra/vertex/pipelines/semantic_relevance:{tag}"
        )

        dataflow_options = [
            "--runner",
            "DataflowRunner",
            "--project",
            project_id,
            "--region",
            "us-east1",
            "--service_account_email",
            f"sa-aiplatform@{project_id}.iam.gserviceaccount.com",
            "--num_workers",
            "1",
            "--max_num_workers",
            num_workers,
            "--flexrs_goal",
            "SPEED_OPTIMIZED",
            "--machine_type",
            "g2-standard-4",
            "--dataflow_service_options",
            "worker_accelerator=type:nvidia-l4;count:1;install-nvidia-driver",
            "--number_of_worker_harness_threads",
            "1",
            "--sdk_container_image",
            image,
            "--experiment",
            "max_workflow_runtime_walltime_seconds=21600",
        ]

        dataflow_gpu_inference_runner = GKEDataflowPythonLaunch(
            task_id=f"run_semrel_inference_{model_name}",
            python_module="semantic_relevance.evaluation.bigquery_metrics",
            image=image,
            service_account_name=SERVICE_ACCOUNT_NAME,
            labels={
                "owner": "search-relevance",
                "pipeline_name": "semantic-relevance-bigquery-metrics",
                "job_trigger": "airflow",
                "job_name": job_name,
            },
            arguments=[
                "--job_name",
                job_id,
                "--date",
                "{{ macros.etsy.hyphenated_days_ago(1, ds) }}",
                "--input_table",
                sampling_table,
                "--model_type",
                model_type,
                "--model_name",
                model_name,
                "--model_path",
                model_path,
                "--bigquery_project_id",
                project_id,
                "--output_table_prefix",
                inference_result_table_prefix,
                "--pairs_table_name",
                inference_output_pairs_table_name,
                "--requests_table_name",
                inference_output_requests_table_name,
                "--batch_size",
                batch_size,
            ]
            + dataflow_options,
        )

        ##  Construct Airflow DAG
        sample_search_requests_for_inference >> dataflow_gpu_inference_runner