import time

from etsy_kubeflow.common.component_spec_generator import (
    PipelineBuilder,
    CustomTrainingComponent,
)
from kfp.v2 import dsl

from kubeflow.settings import Settings
from kubeflow.gpus import get_worker_config
from kubeflow.utils import kfp_deploy_or_run, parse_build_and_pipeline_args


def main():
    # Parse args
    # pipeline args are any args that should be parameterized and who's values may change from run to run
    pipeline_arg_names = [
        "--date",
        "--model-name",
        "--model-path",
        "--batch-size",
    ]
    build_args, pipeline_args = parse_build_and_pipeline_args(pipeline_arg_names)

    # Setting up pipeline function
    pipeline_name = "semantic-relevance-bigquery-metrics"
    timestamp = int(time.time())
    settings = Settings(
        environment=build_args.environment,
        build_method=build_args.build_method,
        tag_name=build_args.tag_name,
        owner="search-relevance",
    )
    worker_config = get_worker_config(
        accelerator="NVIDIA_TESLA_V100",
        accelerator_count=1,
        cpus=8,
        machine_family="standard",
    )
    labels = settings.get_labels(pipeline_name)

    @dsl.pipeline(
        name=pipeline_name,
        description="Calculate model metrics on sampled query-listing pair traffic.",
        pipeline_root=settings.get_pipeline_root(pipeline_name, timestamp),
    )
    def pipeline_fn(
        date: str,
        model_name: str,
        model_path: str,
        batch_size: int,
    ):
        """Definition of KFP pipeline.
        All inputs to this function will be parameterized values set when running the job.

        Args:
            date: Date the model is being trained in YYYY-MM-DD
            model_name: Name of model in BigQuery table
            model_path: Path to model artifact
            batch_size: Inference batch size
        """
        pipeline = PipelineBuilder(container_image=settings.pipeline_image, labels=labels)

        inference_op = CustomTrainingComponent(
            name="inference-and-bq-upload",
            project=settings.project_id,
            python_module="semantic_relevance.evaluation.bigquery_metrics",
            service_account=settings.vertex_service_account,
            inputs={
                "--project-id": settings.project_id,
                "--pairs-table-name": "etsy-data-warehouse-prod.search.sem_rel_query_listing_metrics",
                "--requests-table-name": "etsy-data-warehouse-prod.search.sem_rel_requests_metrics",
            },
            runtime_params={
                "--date": date,
                "--model-name": model_name,
                "--model-path": model_path,
                "--batch-size": batch_size,
            },
            worker_config=worker_config,
            retry_options={"num_retries": 1, "backoff_duration": "1m"},
        )

        pipeline.create_and_add_component(inference_op)

    # Deploy pipeline spec or adhoc run
    kfp_deploy_or_run(
        pipeline_name=pipeline_name,
        pipeline_fn=pipeline_fn,
        build_args=build_args,
        pipeline_args=pipeline_args,
        settings=settings,
        labels=labels,
    )


if __name__ == "__main__":
    main()