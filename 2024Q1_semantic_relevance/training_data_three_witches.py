import numpy as np
import tensorflow as tf
import gcsfs
import pyarrow.parquet as pq
from typing import Dict, Tuple, List
from tqdm import tqdm

FILEPATH = "gs://etldata-prod-search-ranking-data-hkwv8r/feature_logging_training_data_parquet/query_pipeline_web_organic/tight_purchase/_DATE=2024-02-27/results/part-*.parquet"
LISTING_ID = 1532898330


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
    "clientProvidedInfo.query.query",
    "contextualInfo[name=target].docInfo.queryInfo.query",
    "candidateInfo.docInfo.listingInfo.listingId",
    "attributions",
]

# requests is a List[Dict] where each dictionary represents the features for a request.
# requests = load_raw_data_from_parquet_file(
#     filepath=paths[0],
#     columns=columns,
# )

# # first_request is a Dict[str, List] mapping feature names to lists of feature values for each listing in a request.
# first_request = requests[0]

# print("Number of features:", len(first_request))
# len(columns)

# print(first_request)


for j in range(len(paths)):
    requests = load_raw_data_from_parquet_file(
        filepath=paths[j],
        columns=columns,
    )
    for i in range(len(requests)):
        if requests[i]["attributions"] is not None:
            # if "click" in requests[i]["attributions"]:
            if requests[i]["candidateInfo.docInfo.listingInfo.listingId"] is not None:
                if (
                    LISTING_ID
                    in requests[i]["candidateInfo.docInfo.listingInfo.listingId"]
                ):
                    if requests[i]["clientProvidedInfo.query.query"] is not None:
                        print(np.unique(requests[i]["clientProvidedInfo.query.query"]))
                    # if (
                    #     requests[i]["contextualInfo[name=target].docInfo.queryInfo.query"]
                    #     is not None
                    # ):
                    #     print(
                    #         np.unique(
                    #             requests[i][
                    #                 "contextualInfo[name=target].docInfo.queryInfo.query"
                    #             ]
                    #         )
                    #     )
