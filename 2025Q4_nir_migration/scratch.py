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
    "query_clientProvidedInfo.query.query#clientProvidedInfo.query.queryEn#clientProvidedInfo.query.queryCorrected#contextualInfo[name=target].docInfo.queryInfo.query#contextualInfo[name=target].id#fallback#word_2grams",
    "listing_candidateInfo.docInfo.listingInfo.verticaListings.tags#word_2grams",
]
fpath = "gs://training-dev-search-data-jtzn/user/ci/preprocess-test-neural-ir-dedupe-combine-randomize-slim-yzhang/_split=train/30_fef2d51d7f8b4736936d86e743a2ecf8_000000_000000-0.parquet"

data = load_raw_data_from_parquet_file(fpath, columns)
first_request = data[0]
# len(first_request["query_requestUUID"]) # 6 (1+5)
# len(first_request["query_clientProvidedInfo.user.userCountry"]) # 6 (1+5)
# len(first_request["listing_candidateInfo.docInfo.listingInfo.verticaListings.taxonomyPath"]) # 6 (1+5)


listing_feature_data = first_request["listing_candidateInfo.docInfo.listingInfo.verticaListings.tags#word_2grams"]
len(listing_feature_data)
len(listing_feature_data[0])
listing_feature_data[0]
