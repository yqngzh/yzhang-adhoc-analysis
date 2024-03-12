import tensorflow_transform as tft
from datetime import datetime, timedelta
import apache_beam as beam
from third_pass.data_preprocessing import schema
from utils import utils
from utils.eval import (
    PartitionTagsDoFn,
    FlattenJSONWithTags,
    ReplaceDefaultsWithTags,
    PrepareTFTInputWithTags,
    TransformGroupDataWithTags,
    PredictDoFn,
    MetricsDoFn,
)

# https://console.cloud.google.com/vertex-ai/locations/us-central1/pipelines/runs/nr-third-pass-eval-refactor-development-1690466977458?project=etsy-search-ml-dev
model_root = (
    "gs://training-prod-search-data-jtzn/neural_ranking/third_pass/models/nr-third-pass"
)
end_date = "2023-08-15"
date = "2023-08-16"  # eval date
tft_output_path = f"{model_root}/preprocess_fit/{end_date}"
saved_model_path = f"{model_root}/train/{end_date}/main/export/single_device_model"
model_signature = "tensorflow/serving/predict"
config = None
eval_num_of_days = 1
input_base_path_list = [
    "gs://etldata-prod-search-ranking-data-hkwv8r/feature_logging_training_data_purchase_with_predicted_score/query_pipeline_web_organic_sign_in"
]
graphite_prefix_list = ["test"]
partitions = "contextualInfo[name=user].userInfo.userId,contextualInfo[name=target].docInfo.queryInfo.queryLevelMetrics.bin,primaryHardwareType,userCountry,candidateInfo.docInfo.listingInfo.localeFeatures.listingCountry"


eval_end_date = (
    datetime.strptime(date, "%Y-%m-%d") + (timedelta(days=eval_num_of_days - 1))
).strftime("%Y-%m-%d")
input_path_to_prefix_dict = {
    path: prefix for path, prefix in zip(input_base_path_list, graphite_prefix_list)
}
input_paths = utils.get_dated_input_file_list(
    eval_end_date, eval_num_of_days, input_base_path_list
)
tft_output = tft.TFTransformOutput(tft_output_path)
raw_feature_spec = tft_output.raw_feature_spec()
raw_defaults = schema._create_raw_defaults()

eval_end_date
input_paths
raw_feature_spec
raw_defaults
set(raw_feature_spec.keys()) - set(raw_defaults.keys())
set(raw_defaults.keys()) - set(raw_feature_spec.keys())


def print_eval_data(ed, k):
    key_idx = ed._fields.index(k)
    print(len(ed[key_idx]))
    # print(ed[key_idx][0]["contextualInfo"][0]["id"])


# del raw_feature_spec["clientProvidedInfo.query.query"]


# class ScoreListings(beam.DoFn):
#     def __init__(
#         self,
#         saved_model_path: str,
#         raw_feature_spec: Dict,
#     ):
#         self._saved_model_path = saved_model_path
#         self._raw_feature_spec = raw_feature_spec

#     def setup(self):
#         self._model = tf.saved_model.load(self._saved_model_path)
#         self._scoring_fn = self._model.signatures["tensorflow/serving/regress"]

#     def process(self, data: EvalData) -> Iterable[EvalData]:
#         serialized_listings = []
#         for listing in data.flattened_features:
#             serialized_listing = serialize.dict_to_tf_example(
#                 feature_dict=listing,
#                 raw_feature_spec=self._raw_feature_spec,
#                 serialized=True,
#             )
#             serialized_listings.append(serialized_listing)
#         outputs = self._scoring_fn(serialized=tf.constant(serialized_listings))
#         # Shape of ranker_scores is [batch_size]
#         ranker_scores = outputs["scores"]
#         return [
#             data.copy(
#                 ranker_scores=ranker_scores,
#             )
#         ]


with beam.Pipeline() as pipeline:
    predictions = (
        pipeline
        | "Create" >> beam.Create(input_paths)
        | "ReadFiles" >> beam.io.ReadAllFromText(with_filename=True)
        | "ParseJSON" >> beam.Map(lambda x: (x[0], utils.json_loads(x[1])))
        # | "DebugPrint" >> beam.Map(lambda x: print(x[1][0].keys()))
        | "AddPartitionTags"
        >> beam.ParDo(
            PartitionTagsDoFn(
                schema=schema,
                input_path_to_prefix_dict=input_path_to_prefix_dict,
                partitions=partitions,
                attribution_for_sample=None,
                crossing_partitions=None,
                config=config,
            )
        )  # each row of Pcollection is EvalData https://github.com/etsy/neural_ranking/blob/bd1a1427f1d27c851217781b5812ec717ab4a1a8/utils/utils/eval/eval_utils.py#L52
        | "FlattenJSON" >> beam.ParDo(FlattenJSONWithTags(schema=schema, config=config))
        | "DebugPrint" >> beam.Map(lambda x: print_eval_data(x, "raw_features"))
        # | "ReplaceMissingFeatures"
        # >> beam.ParDo(
        #     ReplaceDefaultsWithTags(
        #         raw_feature_spec=raw_feature_spec,
        #         raw_defaults=raw_defaults,
        #     )
        # )
        # | "PrepareTFTInput"
        # >> beam.ParDo(PrepareTFTInputWithTags(raw_feature_spec=raw_feature_spec))
        # | "TransformGroupData"
        # >> beam.ParDo(TransformGroupDataWithTags(tft_output_dir=tft_output_path))
        # | "DebugPrint" >> beam.Map(lambda x: print(type(x[1][0])))  # 48 / 34
        # # | "Predict"
        # # >> beam.ParDo(
        # #     PredictDoFn(schema, saved_model_path, model_signature, tft_output_path)
        # # )
        # # # | "DebugPrint" >> beam.Map(print)
    )
