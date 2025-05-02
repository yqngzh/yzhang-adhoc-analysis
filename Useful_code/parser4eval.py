"""Print eval results to the screen"""

from google.cloud import storage
import pandas as pd
import numpy as np

FILE_PATH = "gs://training-dev-search-data-jtzn/neural_ranking/second_pass/temp/1734722740966/908724743918/yzhang-no-borda-eval-so-web-evaluation-1734722740966/beam-evaluate-saved-model_6562003076671078400/metrics_output_path"
MODEL_NAME = "yzhang-no-borda-eval-so-web"
EVAL_ON_BOE = True

MODEL_NAME = MODEL_NAME.replace("-", "_")
# MODEL_NAME = MODEL_NAME + "_metrics"

if "-web" in MODEL_NAME or "_web" in MODEL_NAME or "-so" in MODEL_NAME or "_so" in MODEL_NAME:
    PLATFORM = "web"
elif "-boe" in MODEL_NAME or "_boe" in MODEL_NAME or "-si" in MODEL_NAME or "_si" in MODEL_NAME:
    PLATFORM = "boe"
else:
    PLATFORM = "web"

print(f"PLATFORM: {PLATFORM}")

## Start running
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
    data_source = f"daily.search.metrics.ranking.tensorflow_ranking_model.{MODEL_NAME}.{PLATFORM}_tight.structured"
elif PLATFORM == "boe":
    if EVAL_ON_BOE:
        print("Eval on BOE")
        data_source = f"daily.boe.metrics.ranking.{MODEL_NAME}.{PLATFORM}_tight.structured"
    else:
        print("Eval on Web SI")
        data_source = f"daily.search.metrics.ranking.tensorflow_ranking_model.{MODEL_NAME}.web_tight.structured"

segments = [
    "userCountry.US",
    # "all",
    # "userId.other",
    # "userId.0",
    # "buyerSegment.Habitual",
    # "buyerSegment.Repeat",
    # "buyerSegment.Active",
    # "buyerSegment.NotActive",
    # "buyerSegment.HighPotential",
    "bin.top_0_01",
    "bin.top_0_1",
    "bin.head",
    "bin.torso",
    "bin.tail",
    "queryIntentSpecScoreV2.2",
    "queryIntentSpecScoreV2.1",
    "queryIntentSpecScoreV2.0",
    "queryIntentSpecScoreV2.MISSING",
]
metric_suffix = [
    "ndcg-purchase.TFRanking.count",
    "ndcg-purchase.TFRanking.mean",
    "dcg-purchase-price.TFRanking.mean",
    "average-price-10.TFRanking.mean",
    "ndcg-click.TFRanking.mean",
    "ndcg-semantic-rel-grade-10.TFRanking.mean",
    "precision-semantic-rel-exact-24.TFRanking.mean",
    "recall-semantic-rel-exact-24.TFRanking.mean",
    "precision-semantic-rel-irr-24.TFRanking.mean",
    "recall-semantic-rel-irr-24.TFRanking.mean",
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
