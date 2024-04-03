import pandas as pd
import numpy as np
import tensorflow as tf
from Useful_code.load_raw_data_from_parquet_file import load_raw_data_from_parquet_file
from Useful_code.run_query_df import run_query_df

test_data = pd.read_csv("./2024Q1_semantic_relevance/semantic_relevance_v1_test.csv")
test_requests = set(list(np.unique(test_data.etsy_uuid)))

# request_mapping_query = f"""SELECT distinct
#     response.mmxRequestUUID,
#     RequestIdentifiers.etsyRequestUUID as etsyUUID,
#     request.query,
# FROM `etsy-searchinfra-gke-prod-2.thrift_mmx_listingsv2search_search.rpc_logs_*`
# WHERE DATE(queryTime) BETWEEN DATE("2023-09-01") AND DATE("2023-09-30")
# AND RequestIdentifiers.etsyRequestUUID IN ("{'", "'.join(list(test_requests))}")
# """

# request_id_mapping_df = run_query_df(request_mapping_query)
# mmx_request_uuids = set(list(request_id_mapping_df.mmxRequestUUID))

FILEPATH = "gs://etldata-prod-search-ranking-data-hkwv8r/feature_logging_training_data_parquet/query_pipeline_web_organic/tight_purchase/_DATE=2023-09-10/results/part-*.parquet"
paths = tf.io.gfile.glob(FILEPATH)

columns = [
    "requestUUID",
    "attributions",
    "candidateInfo.docInfo.listingInfo.listingId",
    "contextualInfo[name=target].docInfo.queryInfo.query",
    "clientProvidedInfo.query.query",
]

for j in range(len(paths)):
    requests = load_raw_data_from_parquet_file(
        filepath=paths[j],
        columns=columns,
    )
    for i in range(len(requests)):
        curr_query = None
        request_uuid_vec = requests[i]["requestUUID"]
        client_query_vec = requests[i]["clientProvidedInfo.query.query"]
        context_query_vec = requests[i]["clientProvidedInfo.query.query"]
        listing_id_vec = requests[i]["candidateInfo.docInfo.listingInfo.listingId"]
        if client_query_vec is not None:
            curr_query = client_query_vec[0]
        elif context_query_vec is not None:
            curr_query = context_query_vec[0]

        if curr_query is not None:
            sub_df = test_data[test_data["query"] == curr_query]
            if sub_df.shape[0] > 0:
                print(f"{i=}, {j=}, {curr_query=}, {request_uuid_vec[0]=}")
                listings_with_labels = set(sub_df.listingId.unique())
                if listing_id_vec is not None:
                    count = 0
                    for k in range(len(listing_id_vec)):
                        if listing_id_vec[k] in listings_with_labels:
                            count += 1
                    print(f"{count=}, {len(listing_id_vec)=}")
