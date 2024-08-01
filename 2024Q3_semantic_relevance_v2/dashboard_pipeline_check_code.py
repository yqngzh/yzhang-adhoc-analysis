import datetime
from airflow import DAG
from etsy.operators.vertex_ai import EtsyVertexAIPipelineJobOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryExecuteQueryOperator,
)
from etsy.utils.defaults import patch_all

patch_all()

DEFAULT_TASK_ARGS = {
    "sla": datetime.timedelta(days=1),
    "owner": "search",
    "retries": 1,
    "retry_delay": datetime.timedelta(seconds=300),
    "start_date": datetime.datetime(2024, 5, 10, 0, 0),
    "dataproc_hadoop_jars": ["{{ macros.etsy.latest_scalding_build_jar(task) }}"],
    "dag_monitor_email": ["mmatsui@etsy.com"],
}

with DAG(
    dag_id="semantic_relevance_per_exp",
    catchup=True,
    max_active_runs=10,
    max_active_tasks=10,
    schedule="@daily",
    default_args=DEFAULT_TASK_ARGS,
) as dag:
    sample_search_requests_for_inference = BigQueryExecuteQueryOperator(
        task_id="sample_search_requests_for_inference",
        sql="SELECT * FROM `etsy-data-warehouse-dev.search.sem_rel_sample_requests_per_exp`('{{macros.etsy.hyphenated_days_ago(1,ds)}}', ['ranking/search.perso_engine.quality_bet_v3_boe', 'ranking/search.perso_engine.quality_bet_v3_web'], 5000, FALSE)",
        destination_dataset_table="etsy-data-warehouse-dev.search.sem_rel_daily_exp",
        write_disposition="WRITE_APPEND",
        create_disposition="CREATE_IF_NEEDED",
        time_partitioning={"field": "date", "type": "DAY"},
        use_legacy_sql=False,
        location="US",
    )

    timestamp = datetime.datetime.now(datetime.UTC).strftime("%Y%m%d-%H%M%S")
    pipeline_name = "semrel-daily-exp"
    model_name = "bert-cern-l24-h1024-a16"
    job_name = f"{pipeline_name}-{model_name}"
    job_id = f"{job_name}-{timestamp}"
    tag = "teacher1"
    model_path = "gs://training-dev-search-data-jtzn/semantic_relevance/production_models/bert-cern-l24-h1024-a16-v1/export/saved_model"
    batch_size = 2048
    pipeline_def_path = f"gs://training-dev-search-data-jtzn/kfp_pipeline_spec/semantic_relevance/tag/{tag}/semantic-relevance-bigquery-metrics_pipeline_spec.json"
    run_inference_pipeline = EtsyVertexAIPipelineJobOperator(
        task_id="sampled_requests_run_inference",
        job_id=job_id,
        project_id="etsy-search-ml-prod",
        impersonation_chain="sa-aiplatform@etsy-search-ml-prod.iam.gserviceaccount.com",
        service_account="sa-aiplatform@etsy-search-ml-prod.iam.gserviceaccount.com",
        pipeline_def_path=pipeline_def_path,
        labels={
            "owner": "search-relevance",
            "pipeline_name": pipeline_name,
            "job_trigger": "airflow",
            "job_name": job_name,
        },
        enable_caching=False,
        pipeline_params={
            "date": "{{macros.etsy.hyphenated_days_ago(1,ds)}}",
            "model_name": model_name,
            "model_path": model_path,
            "batch_size": batch_size,
        },
    )

    sample_search_requests_for_inference >> run_inference_pipeline
