import tensorflow as tf
from Useful_code.load_raw_data_from_parquet_file import load_raw_data_from_parquet_file

FILEPATH = "gs://etldata-prod-search-ranking-data-hkwv8r/user/yzhang/semantic_relevance/curated_eval_data_v1/_DATE=2023-09-30/results/part-*"
paths = tf.io.gfile.glob(FILEPATH)

columns = [
    "requestUUID",
    "attributions",
    "candidateInfo.docInfo.listingInfo.listingId",
    "contextualInfo[name=target].docInfo.queryInfo.query",
    "clientProvidedInfo.query.query",
]

requests = load_raw_data_from_parquet_file(
    filepath=paths[0],
    columns=columns,
)
first_request = requests[0]

count_request = 0
for j in range(len(paths)):
    requests = load_raw_data_from_parquet_file(
        filepath=paths[j],
        columns=columns,
    )
    count_request += len(requests)

count_request
