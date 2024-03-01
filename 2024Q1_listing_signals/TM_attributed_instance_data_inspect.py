import tensorflow as tf
from Useful_code.view_raw_training_data_parquet import load_raw_data_from_parquet_file

FILEPATH = "gs://etldata-prod-search-ranking-data-hkwv8r/user/yzhang/listing_signals_instances_try1/query_pipeline_web_organic/FilteredFlattenedAttributedInstance/tight_purchase/_DATE=2024-01-10/part-*.parquet"

paths = tf.io.gfile.glob(FILEPATH)

columns = ["timeSinceEpochMs", "fakeTimestampInMillis"]

requests = load_raw_data_from_parquet_file(
    filepath=paths[0],
    columns=columns,
)
