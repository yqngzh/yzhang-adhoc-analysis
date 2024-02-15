import numpy as np
import tensorflow as tf
import gcsfs
import pyarrow.parquet as pq
from typing import Dict, List
from tqdm import tqdm

FILEPATH = "gs://etldata-prod-search-ranking-data-hkwv8r/user/yzhang/listing_signals_v3/feature_logging_training_data_parquet/query_pipeline_boe_organic/tight_purchase/_DATE=2024-01-12/results/part-*.parquet"


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
    "contextualInfo[name=user].rivuletUserInfo.timeseries.recentlyVideoPlayedListingIds50FV1#listingId",
    "contextualInfo[name=user].rivuletUserInfo.timeseries.recentlyViewedListingIds50FV1#listingId",
]

for j in tqdm(range(len(paths))):
    requests = load_raw_data_from_parquet_file(
        filepath=paths[j],
        columns=columns,
    )
    for i in range(len(requests)):
        feature = requests[i][
            "contextualInfo[name=user].rivuletUserInfo.timeseries.recentlyVideoPlayedListingIds50FV1#listingId"
        ]
        if feature is not None:
            print(feature)

requests[0][
    "contextualInfo[name=user].rivuletUserInfo.timeseries.recentlyViewedListingIds50FV1#listingId"
]
