# under nrv2 environment
# cd ~/development/neural_ranking/second_pass
import apache_beam as beam
import tensorflow as tf
from utils.preprocess.config import load_fit_config
from utils.preprocess.data_loading import (
    load_raw_data_from_parquet_file,
    load_schema_from_parquet_file,
)

# date = "2025-05-06"
date = "2025-04-30"
preprocess_config_path = "pipeline/configs/preprocess/sr_si.yaml|prod"
filtered_filepath = f"gs://etldata-prod-search-ranking-data-hkwv8r/tm/version=1/feature_logging_training_data_parquet/query_pipeline_market_web_organic/cartadd_v1/_DATE={date}/results/part-*"
full_filepath = f"gs://etldata-prod-search-ranking-data-hkwv8r/tm/version=1/attributed_training_data_parquet/query_pipeline_web_organic/FilteredFlattenedAttributedInstance/tight/_DATE={date}/part-*"

### Check schema
paths = tf.io.gfile.glob(filtered_filepath)
# paths = tf.io.gfile.glob(full_filepath)
path = paths[0]
schema = load_schema_from_parquet_file(path)
requests = load_raw_data_from_parquet_file(
    filepath=path,
)
first_request = requests[0]
first_request["candidateInfo.retrievalInfo.semrelRelevantScore"]


### Count n requests
def run_dataflow()
    config = load_fit_config(preprocess_config_path)

    with beam.Pipeline(options=pipeline_options) as pipeline:
        with beam_impl.Context(temp_dir=f""):
            (
                pipeline
                | "CreateFileList" >> beam.Create([filtered_filepath])
                | "ReadFiles"
                >> beam.io.ReadAllFromParquet(
                    with_filename=False,
                    columns=list(config.features_and_labels.keys()),
                )
                | "GroupByRequests" >> beam.GroupBy(lambda x: x["requestUUID"])
                | "CountNumRows" >> beam.combiners.Count.Globally()
                | "PrintToLogs" >> beam.Map(lambda x: logging.info(f"Number of rows: {x}"))
            )
    
# filtered: 
# full: 
# bigquery 
# # select count(distinct requestUUID)
# # from `etsy-ml-systems-prod.attributed_instance.query_pipeline_web_organic_tight_2025_05_06`
# 1685125 requests

