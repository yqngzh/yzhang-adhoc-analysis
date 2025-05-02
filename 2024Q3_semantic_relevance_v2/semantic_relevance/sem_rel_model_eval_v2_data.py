import gcsfs
import pandas as pd
import numpy as np
import tensorflow as tf
import tensorflow_text as text
from google.cloud import bigquery
from tqdm import tqdm
import requests
import json

#### Load models
TEACHER_PATH = "gs://training-dev-search-data-jtzn/semantic_relevance/production_models/bert-cern-l24-h1024-a16-v1/export/saved_model"
INTEGRATION_PATH = "gs://training-dev-search-data-jtzn/semantic_relevance/production_models/bert-cern-l2-h128-a2-v1/export/saved_model"

teacher_model = tf.saved_model.load(TEACHER_PATH)
integration_model = tf.saved_model.load(INTEGRATION_PATH)

RETRIEVAL_URL = "https://prod-ml-platform.etsycloud.com/barista/semrel-filter/v1/models/semrel-filter:predict"

class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NpEncoder, self).default(obj)
    

#### Predict on V1 data
V1_TEST_DATA = "gs://training-dev-search-data-jtzn/semantic_relevance/datasets/human_annotation_v1_split_v1/annotation_dataset_test_v1.pq"

fs = gcsfs.GCSFileSystem()
df_v1 = pd.read_parquet(V1_TEST_DATA, filesystem=fs)
df_v1 = df_v1[["query", "listingId", "title", "gold_label"]].drop_duplicates()

print(df_v1.shape[0])
print(len(df_v1["query"].drop_duplicates()))

queries = list(df_v1["query"].values)
titles = list(df_v1["title"].values)

teacher_outputs = teacher_model.signatures["serving_default"](
    queries=queries,
    titles=titles,
)

df_v1["teacher_label"] = tf.argmax(teacher_outputs["softmax"], axis=1).numpy() + 1
df_v1["teacher_score_1"] = teacher_outputs["softmax"][:, 0].numpy()
df_v1["teacher_score_4"] = teacher_outputs["softmax"][:, 3].numpy()

integration_outputs = integration_model.signatures["serving_default"](
    queries=queries,
    titles=titles,
)

df_v1["integration_label"] = tf.argmax(integration_outputs["softmax"], axis=1).numpy() + 1
df_v1["integration_score_1"] = integration_outputs["softmax"][:, 0].numpy()
df_v1["integration_score_4"] = integration_outputs["softmax"][:, 3].numpy()

retrieval_pred_label = []
retrieval_pred_score = []
for i in range(df_v1.shape[0]):
    curr_query_str = queries[i]
    curr_listing_id = df_v1["listingId"].values[i]
    payload_data = {
        "signature_name": "score_listings", 
        "inputs": {
            "query": curr_query_str, 
            "listing_ids":[curr_listing_id]
        }
    }
    response = requests.post(RETRIEVAL_URL, data=json.dumps(payload_data, cls=NpEncoder))
    model_pred = response.json()
    if len(model_pred['outputs']['missing_listing_ids']) > 0:
        print(model_pred)
        listing_score = np.nan
        curr_label = np.nan
    else:
        listing_score = model_pred['outputs']['scores'][0]
        shop_score = model_pred['outputs']['shop_scores'][0]
        curr_label = 1 if (listing_score < 0.45 and shop_score < 0.5) else 4
    retrieval_pred_label.append(curr_label)
    retrieval_pred_score.append(listing_score)

df_v1["retrieval_label"] = retrieval_pred_label
df_v1["retrieval_score"] = retrieval_pred_score

df_v1.to_csv("/home/yzhang/development/0_yzhang_local/semantic_relevance/v1_model_preds.csv", index=False)


#### Predict on V2 data
query_str = """select distinct query, listingTitle, listingId, relevance_value
from `etsy-search-ml-dev.human_annotation.survey2_examples_w_labels`
where relevance_value not like 'not_sure%'
"""
client = bigquery.Client(project="etsy-search-ml-dev")
query_job = client.query(query_str)
rows = query_job.result()
df_v2 = rows.to_dataframe()

rel_label_to_int = {
    "relevant": 4,
    "partial": 2,
    "not_relevant": 1
}
df_v2["gold_label"] = df_v2["relevance_value"].apply(lambda x: rel_label_to_int[x])

queries = list(df_v2["query"].values)
titles = list(df_v2["listingTitle"].values)

def batch_pred(query_vec, title_vec, model, batch_size=1024):
    out_label = []
    out_score_1 = []
    out_score_4 = []
    num_batch = len(query_vec) // batch_size + 1
    for i in tqdm(range(num_batch)):
        start_idx = i * batch_size
        end_idx = min((i + 1) * batch_size, len(query_vec))
        outputs = model.signatures["serving_default"](
            queries=query_vec[start_idx:end_idx],
            titles=title_vec[start_idx:end_idx],
        )
        out_label.extend(tf.argmax(outputs["softmax"], axis=1).numpy() + 1)
        out_score_1.extend(outputs["softmax"][:, 0].numpy())
        out_score_4.extend(outputs["softmax"][:, 3].numpy())
    return out_label, out_score_1, out_score_4


teacher_label, teacher_score_1, teacher_score_4 = batch_pred(queries, titles, teacher_model)
df_v2["teacher_label"] = teacher_label
df_v2["teacher_score_1"] = teacher_score_1
df_v2["teacher_score_4"] = teacher_score_4

integration_label, integration_score_1, integration_score_4 = batch_pred(queries, titles, integration_model)
df_v2["integration_label"] = integration_label
df_v2["integration_score_1"] = integration_score_1
df_v2["integration_score_4"] = integration_score_4

retrieval_pred_label = []
retrieval_pred_score = []
for i in range(df_v2.shape[0]):
    curr_query_str = df_v2["query"].values[i]
    curr_listing_id = df_v2["listingId"].values[i]
    payload_data = {
        "signature_name": "score_listings", 
        "inputs": {
            "query": curr_query_str, 
            "listing_ids":[curr_listing_id]
        }
    }
    response = requests.post(RETRIEVAL_URL, data=json.dumps(payload_data, cls=NpEncoder))
    model_pred = response.json()
    if len(model_pred['outputs']['missing_listing_ids']) > 0:
        print(model_pred)
        listing_score = np.nan
        curr_label = np.nan
    else:
        listing_score = model_pred['outputs']['scores'][0]
        shop_score = model_pred['outputs']['shop_scores'][0]
        curr_label = 1 if (listing_score < 0.45 and shop_score < 0.5) else 4
    retrieval_pred_label.append(curr_label)
    retrieval_pred_score.append(listing_score)

df_v2["retrieval_label"] = retrieval_pred_label
df_v2["retrieval_score"] = retrieval_pred_score

df_v2.to_csv("/home/yzhang/development/0_yzhang_local/semantic_relevance/v2_model_preds.csv", index=False)
