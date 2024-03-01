import numpy as np
import tensorflow as tf
import gcsfs
import pyarrow.parquet as pq
from typing import Dict, Tuple, List
from tqdm import tqdm

FILEPATH = "gs://etldata-prod-search-ranking-data-hkwv8r/user/yzhang/listing_signals_v5/feature_logging_training_data_parquet/query_pipeline_boe_organic/tight_purchase/_DATE=2024-01-15/results/part-*.parquet"


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
    "candidateInfo.docInfo.listingInfo.listingWeb.hasVideo",
    "candidateInfo.docInfo.listingInfo.listingWeb.isFreeShipping#keys",
    "candidateInfo.docInfo.listingInfo.listingWeb.isFreeShipping#values",
    "candidateInfo.docInfo.listingInfo.listingWeb.isBestseller",
    "candidateInfo.docInfo.listingInfo.listingWeb.isLimitedQuantity",
    "candidateInfo.docInfo.listingInfo.listingWeb.isEtsyPick",
    "candidateInfo.docInfo.listingInfo.listingWeb.price#keys",
    "candidateInfo.docInfo.listingInfo.listingWeb.price#values",
    "candidateInfo.docInfo.listingInfo.listingWeb.promotionalPrice#keys",
    "candidateInfo.docInfo.listingInfo.listingWeb.promotionalPrice#values",
    "candidateInfo.docInfo.listingInfo.listingWeb.quantity",
    "candidateInfo.docInfo.listingInfo.listingWeb.tags",
    "candidateInfo.docInfo.listingInfo.listingWeb.isDigital",
    "candidateInfo.docInfo.listingInfo.listingWeb.isActive",
    "candidateInfo.docInfo.listingInfo.listingWeb.isBlockedZeroReccos",
    "candidateInfo.docInfo.listingInfo.listingWeb.isSearchable",
    "candidateInfo.docInfo.listingInfo.listingWeb.isAvailable",
    "candidateInfo.docInfo.listingInfo.listingWeb.isDisplayable",
    "contextualInfo[name=user].rivuletUserInfo.timeseries.recentlyVideoPlayedListingIds50FV1#listingId",
]

# requests is a List[Dict] where each dictionary represents the features for a request.
requests = load_raw_data_from_parquet_file(
    filepath=paths[0],
    columns=columns,
)

# first_request is a Dict[str, List] mapping feature names to lists of feature values for each listing in a request.
first_request = requests[0]

print("Number of features:", len(first_request))
len(columns)

# print features by name
for feature_name, feature_values in first_request.items():
    if "rivuletUserInfo.timeseries" in feature_name:
        print(feature_name)
    # print(feature_values)

print(first_request["candidateInfo.docInfo.listingInfo.listingWeb.isDigital"])
print(first_request["candidateInfo.docInfo.listingInfo.listingWeb.isSearchable"])
print(
    first_request["candidateInfo.docInfo.listingInfo.listingWeb.promotionalPrice#keys"]
)


for j in tqdm(range(len(paths))):
    requests = load_raw_data_from_parquet_file(
        filepath=paths[j],
        columns=columns,
    )
    for i in range(len(requests)):
        feature = requests[i]["candidateInfo.docInfo.listingInfo.listingWeb.isDigital"]
        if feature is not None:
            print(feature)
