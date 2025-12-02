# to run in etsyml

import gcsfs
import pyarrow.parquet as pq
from typing import Dict, List


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


columns = [
    "query_requestUUID",
    "query_clientProvidedInfo.user.userCountry",
    "listing_candidateInfo.docInfo.listingInfo.verticaListings.taxonomyPath"
]
fpath = "gs://training-dev-search-data-jtzn/user/ci/preprocess-test-neural-ir-dedupe-combine-nesting-asherrick/_split=train/315_e1691320bd6e47de8d94262828bc33a5_000000_000000-0.parquet"

data = load_raw_data_from_parquet_file(fpath, columns)
first_request = data[0]
len(first_request["query_requestUUID"]) # 6 (1+5)
len(first_request["query_clientProvidedInfo.user.userCountry"]) # 6 (1+5)
len(first_request["listing_candidateInfo.docInfo.listingInfo.verticaListings.taxonomyPath"]) # 6 (1+5)
