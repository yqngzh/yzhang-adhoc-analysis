import numpy as np
import tensorflow as tf
import gcsfs
import pyarrow.parquet as pq
from typing import Dict, Tuple, List
from tqdm import tqdm


FILEPATH = "gs://etldata-prod-search-ranking-data-hkwv8r/user/yzhang/listing_signals_v5/feature_logging_training_data_parquet/query_pipeline_web_organic/tight_purchase/_DATE=2024-03-18/results/part-*.parquet"
# FILEPATH = "gs://etldata-prod-search-ranking-data-hkwv8r/feature_logging_training_data_parquet/query_pipeline_web_organic/tight_purchase/_DATE=2024-01-21/results/part-*.parquet"


def load_raw_data_from_parquet_file(
    filepath: str,
    columns: List[str] = None,
) -> List[Dict]:
    if "gs://" in filepath:
        fs = gcsfs.GCSFileSystem()
        filepath = filepath.replace("gs://", "")
    else:
        fs = None
    df = pq.read_table(
        source=filepath,
        filesystem=fs,
        use_pandas_metadata=True,
        columns=columns,
    )
    data = df.to_pylist()
    return data


paths = tf.io.gfile.glob(FILEPATH)

columns = [
    "attributions",
    "clientProvidedInfo.query.query",
    "contextualInfo[name=target].docInfo.queryInfo.query",
    "contextualInfo[name=target].docInfo.queryInfo.queryLevelMetrics.isDigital",
    "candidateInfo.docInfo.listingInfo.listingWeb.isDigital",
]

query_with_digital_purchase_list = []

for i in tqdm(range(len(paths))):
    requests = load_raw_data_from_parquet_file(
        filepath=paths[i],
        columns=columns,
    )
    for j in range(len(requests)):
        request_attributions = requests[j]["attributions"]
        request_queries = requests[j]["clientProvidedInfo.query.query"]
        query_is_digital = requests[j]["contextualInfo[name=target].docInfo.queryInfo.queryLevelMetrics.isDigital"]
        listing_is_digital = requests[j]["candidateInfo.docInfo.listingInfo.listingWeb.isDigital"]
        query_with_digital_purchase = False
        for k in range(len(request_attributions)):
            if request_attributions is None or request_queries is None or query_is_digital is None or listing_is_digital is None:
                query_with_digital_purchase = False
            elif "purchase" in request_attributions[k] and query_is_digital[k] == 0 and listing_is_digital[k]:
                query_with_digital_purchase = True
        if query_with_digital_purchase:
            query_with_digital_purchase_list.append(request_queries[0])
                
query_with_digital_purchase_list = list(set(query_with_digital_purchase_list))

with open('query_with_digital_purchase.txt', 'w') as f:
    for q in query_with_digital_purchase_list:
        f.write(f"{q}\n")
