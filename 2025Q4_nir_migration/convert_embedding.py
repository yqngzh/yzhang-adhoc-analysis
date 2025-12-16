import tensorflow as tf
import pandas as pd

spec = {
    "listingId": tf.io.FixedLenFeature([], dtype=tf.int64),
    "embedding": tf.io.FixedLenFeature([256], tf.float32),
}

def parse_function(example_proto):
    return tf.io.parse_single_example(example_proto, spec)

dataset = tf.data.TFRecordDataset("gs://training-dev-search-data-jtzn/user/yzhang/listing_embedding_data/_DATE=2025-11-01/part-00000-of-00159")
parsed_dataset = dataset.map(parse_function)

data_list = []
for element in parsed_dataset.as_numpy_iterator():
    data_list.append({key: value for key, value in element.items()})

len(data_list)
df = pd.DataFrame(data_list)
df.to_parquet('~/development/yzhang-adhoc-analysis/2025Q4_nir_migration/part-00000-of-00159.parquet', engine='pyarrow')

# gsutil cp ~/development/yzhang-adhoc-analysis/2025Q4_nir_migration/part-00000-of-00159.parquet gs://training-dev-search-data-jtzn/user/yzhang/listing_embedding_parquet/_DATE=2025-11-01/
