"""Print eval results to the screen"""

from google.cloud import storage
import pandas as pd
import numpy as np

FILE_PATH = "gs://training-dev-search-data-jtzn/neural_ranking/second_pass/temp/1710264131217/908724743918/yz-rrp-gains-web-30d-development-1710264131217/beam-evaluate-trained-model_4836447154585206784/metrics_output_path"
MODEL_NAME = "yz-rrp-gains-web-30d"

if "-web-" in MODEL_NAME or "_web_" in MODEL_NAME:
    PLATFORM = "web"
elif "-boe-" in MODEL_NAME or "_boe_" in MODEL_NAME:
    PLATFORM = "boe"

input_map = {}
file_tokens = FILE_PATH.split("/")
bucket_name = file_tokens[2]
path = "/".join(file_tokens[3:])
client = storage.Client()
bucket = client.get_bucket(bucket_name)
blob = bucket.get_blob(path)
input = blob.download_as_string()
inputs = input.splitlines()
inputs = [x.decode("utf-8") for x in inputs]
for input in inputs:
    parts = input.split(",")
    input_map[parts[0]] = parts[1]

if PLATFORM == "web":
    data_source = f"daily.search.metrics.ranking.tensorflow_ranking_model.{MODEL_NAME.replace('-', '_')}.{PLATFORM}_tight.structured"
elif PLATFORM == "boe":
    data_source = f"daily.boe.metrics.ranking.{MODEL_NAME.replace('-', '_')}.{PLATFORM}_tight.structured"

segments = [
    # "all",
    "userCountry.US",
    "userId.other",
    "userId.0",
    "buyerSegment.Habitual",
    "buyerSegment.Repeat",
    "buyerSegment.Active",
    "buyerSegment.NotActive",
    "bin.top_0_01",
    "bin.top_0_1",
    "bin.head",
    "bin.torso",
    "bin.tail",
]
metric_suffix = [
    "ndcg-purchase.TFRanking.count",
    "ndcg-purchase.TFRanking.mean",
    "dcg-purchase-price.TFRanking.mean",
    # "average-price-3.TFRanking.mean",
    # "average-price-10.TFRanking.mean",
    # "average-price-24.TFRanking.mean",
]
list_metrics = []
for i in range(len(segments)):
    for j in range(len(metric_suffix)):
        list_metrics.append(segments[i] + "." + metric_suffix[j])

metric_names = []
metric_values = []

for i in range(len(list_metrics)):
    metric_name = data_source + "." + list_metrics[i]
    if "count" not in metric_name:
        try:
            metric_value = str(np.around(float(input_map[metric_name]), decimals=4))
        except KeyError:
            metric_value = str("#N/A")
    else:
        try:
            metric_value = input_map[metric_name]
        except KeyError:
            metric_value = str(0)
    metric_names.append(metric_name)
    metric_values.append(metric_value)

pd.options.display.max_colwidth = 100
df = pd.DataFrame({"metric_names": metric_names, "metric_values": metric_values})

print(",".join(metric_values))
