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


columns = None
# columns = [
#     "query_clientProvidedInfo.query.query#clientProvidedInfo.query.queryEn#clientProvidedInfo.query.queryCorrected#contextualInfo[name=target].docInfo.queryInfo.query#contextualInfo[name=target].id#fallback#word_2grams",
#     "listing_candidateInfo.docInfo.listingInfo.verticaListings.tags#word_2grams",
# ]

# fpath = "gs://training-dev-search-data-jtzn/user/ci/preprocess-test-neural-ir-dedupe-combine-randomize-slim-yzhang/_split=train/30_fef2d51d7f8b4736936d86e743a2ecf8_000000_000000-0.parquet"
# fpath = "gs://etldata-prod-adhoc-data-hkwv8r/data/shared/neural_ir/all/sample_for_testing/negatives/_DATE=2025-11-01/part-02402-8f7ceeb5-858f-421e-a698-a4e7d24b77b2-c000.gz.parquet"
# fpath = "gs://etldata-prod-adhoc-data-hkwv8r/data/shared/neural_ir/all/sample_for_testing/negatives/_DATE=2025-11-01/part-14840-a6d41b5f-1556-41b4-a0c1-c11bd349c419-c000.gz.parquet"
# fpath = "gs://etldata-prod-search-ranking-data-hkwv8r/data/shared/neural_ir/all/ListingFeatures/nir_flat_pos/v2/query_pipeline_web_organic/tight_purchase/_DATE=2025-12-09/part-00199-ff0d2ae3-18db-49c9-9767-9304154f7f9b-c000.gz.parquet"
# fpath = "gs://etldata-prod-search-ranking-data-hkwv8r/data/shared/neural_ir/all/ListingFeatures/active_listings/v2/_DATE=2025-12-09/part-03499-25dd0037-e493-4bf8-aa90-53baa14a8270-c000.snappy.parquet"
# fpath = "gs://training-dev-search-data-jtzn/user/ci/preprocess-test-neural-ir-main-yzhang/_split=train/30_d9939ad684e9415eafda2f8a5a1628fb_000000_000000-0.parquet"
# fpath = "gs://training-dev-search-data-jtzn/user/user/nir_processed_2026-01-16/_split=train/1322_09abafc2612e4b47900f75f1c5fdfe2f_005949_000000-0.parquet"
fpath = "gs://training-dev-search-data-jtzn/user/yzhang/yzhang-data-statictokenizer/_split=train/62_f75502e0c3a34ef08a449702b4de27da_004534_000000-0.parquet"

data = load_raw_data_from_parquet_file(fpath, columns)
first_request = data[0]
first_request.keys()
# len(first_request["query_requestUUID"]) # 6 (1+5)
# len(first_request["query_clientProvidedInfo.user.userCountry"]) # 6 (1+5)
# len(first_request["listing_candidateInfo.docInfo.listingInfo.verticaListings.taxonomyPath"]) # 6 (1+5)


listing_feature_data = first_request["listing_candidateInfo.docInfo.listingInfo.verticaListings.tags#word_2grams"]
len(listing_feature_data)
len(listing_feature_data[0])
listing_feature_data[0]


import torch
import numpy as np
import gcsfs
from models.neural_ir.config import NeuralIR

tensor_request = {
    'query_clientProvidedInfo.query.query#clientProvidedInfo.query.queryEn#clientProvidedInfo.query.queryCorrected#contextualInfo[name=target].docInfo.queryInfo.query#contextualInfo[name=target].id#fallback#word_2grams': torch.from_numpy(np.array([[]], dtype=np.int64)), 
    'query_clientProvidedInfo.user.userPreferredLanguage#hash_6_7': torch.tensor([[552, 660]]), 
    'query_clientProvidedInfo.user.userCountry#hash_2_3': torch.tensor([[ 78, 191]]), 
    'query_contextualInfo[name=user].userInfo.activeUserShipToLocations.latitude#clientProvidedInfo.user.locationLatitude#fallback:log1p': torch.tensor([0]), 
    'query_contextualInfo[name=user].userInfo.activeUserShipToLocations.longitude#clientProvidedInfo.user.locationLongitude#fallback:log1p': torch.tensor([0]), 
    'listing_candidateInfo.docInfo.listingInfo.activeListingLocation.longitude:log1p': torch.tensor([-4]), 
    'listing_candidateInfo.docInfo.listingInfo.activeListingLocation.latitude:log1p': torch.tensor([3]), 
    'listing_candidateInfo.docInfo.listingInfo.verticaListings.title#candidateInfo.docInfo.listingInfo.verticaListingTranslations.primaryLanguageTitle#fallback#word_2grams': torch.tensor([[79732, 63359, 29907, 47750]]), 
    'listing_candidateInfo.docInfo.shopInfo.verticaShopSettings.primaryLanguage#hash_6_7': torch.tensor([[875, 549]]), 
    'listing_candidateInfo.docInfo.listingInfo.activeListingBasics.priceUsd:log1p': torch.tensor([3]), 
    'listing_candidateInfo.docInfo.listingInfo.verticaListings.tags#word_2grams': torch.from_numpy(np.array([[]], dtype=np.int64)), 
    'listing_candidateInfo.docInfo.listingInfo.activeListingBasics.maxPriceUsd:log1p': torch.tensor([0]), 
    'listing_candidateInfo.docInfo.listingInfo.localeFeatures.listingCountry#hash_2_3': torch.tensor([[148, 642]])
}


model = NeuralIR().model()
model_path = "gs://training-dev-search-data-jtzn/user/ci/neuralir-test-100K-toks-asherrick/train/checkpoint_20/model.pt"
fs = gcsfs.GCSFileSystem()
model_path = model_path.replace("gs://", "")
with fs.open(model_path, "rb") as f:
    state_dict = torch.load(f, map_location="cpu")

model.load_state_dict(state_dict, strict=False)

model(tensor_request)
