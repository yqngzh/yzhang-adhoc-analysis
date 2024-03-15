import numpy as np
import tensorflow as tf
import gcsfs
import pyarrow.parquet as pq
from typing import Dict, Tuple, List
from tqdm import tqdm


FILEPATH = "gs://etldata-prod-search-ranking-data-hkwv8r/user/yzhang/listing_signals_v5/feature_logging_training_data_parquet/query_pipeline_boe_organic/tight_purchase/_DATE=2024-03-06/results/part-*.parquet"
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
    "requestUUID",
    "attributions",
    "candidateInfo.docInfo.listingInfo.listingWeb.isDigital",
]

# requests is a List[Dict] where each dictionary represents the features for a request.
request_count = 0  # 89881
purchase_counts = 0  # 93596
digital_listing_purchase_counts = 0  # 21241 (22%)
total_context_listing_counts = 0  # 2542580
for i in tqdm(range(len(paths))):
    requests = load_raw_data_from_parquet_file(
        filepath=paths[i],
        columns=columns,
    )
    request_count += len(requests)
    for j in range(len(requests)):
        total_context_listing_counts += len(requests[j]["attributions"])
        for k in range(len(requests[j]["attributions"])):
            if "purchase" in requests[j]["attributions"][k]:
                purchase_counts += 1
                if (
                    requests[j][
                        "candidateInfo.docInfo.listingInfo.listingWeb.isDigital"
                    ]
                    is not None
                ):
                    if requests[j][
                        "candidateInfo.docInfo.listingInfo.listingWeb.isDigital"
                    ][k]:
                        digital_listing_purchase_counts += 1
