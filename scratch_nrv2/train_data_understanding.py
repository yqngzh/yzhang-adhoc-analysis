import tensorflow as tf
import numpy as np
import json
from neural_ranking.third_pass.third_pass.data_preprocessing.schema import (
    flatten_json,
    _create_feature_spec,
)
from utils.utils import json_loads
import apache_beam as beam
from tensorflow_ranking.python.keras.pipeline import (
    DatasetHparams,
    SimpleDatasetBuilder,
)

## raw training data
part_file = "gs://etldata-prod-search-ranking-data-hkwv8r/feature_logging_training_data_purchase_with_predicted_score/query_pipeline_web_organic/2023-07-23/results/part-00887-of-00888"
grouped_docs = []
with tf.io.gfile.GFile(part_file) as f:
    for line in f:
        curr_res = json.loads(line)
        grouped_docs.append(curr_res)
        if len(grouped_docs) >= 1:
            break

len(curr_res)
curr_res[0].keys()
print(
    json.dumps(
        {
            k: v
            for k, v in curr_res[0].items()
            if k
            in ["requestUUID", "position", "attributions", "visitID", "attribution"]
        },
        indent=4,
    )
)

curr_line = curr_res

request_uuid_seq = []
position_seq = []
visit_id_seq = []
page_num_seq = []
visit_user_query_id = []
query_seq = []

for i in range(len(curr_line)):
    request_uuid_seq.append(curr_line[i]["requestUUID"])
    position_seq.append(curr_line[i]["position"])
    visit_id_seq.append(curr_line[i]["visitId"])
    query_seq.append(curr_line[i]["contextualInfo"][0]["docInfo"]["queryInfo"]["query"])

print(len(request_uuid_seq))
print(np.unique(request_uuid_seq))
print(len(position_seq))
print(np.unique(position_seq))
print(len(visit_id_seq))
print(np.unique(visit_id_seq))
print(len(visit_user_query_id))
print(np.unique(visit_user_query_id))
print(len(query_seq))
print(np.unique(query_seq))
# a visit may have more than 1 request (query, page)


## preprocess fit
flattened_res = flatten_json(curr_res[0])
print(json.dumps(flattened_res, indent=4))


## preprocess generate
with beam.Pipeline() as pipeline:
    encoded_dataset = (
        pipeline
        | "CreateFileList"
        >> beam.Create(
            [
                "gs://etldata-prod-search-ranking-data-hkwv8r/feature_logging_training_data_purchase_with_predicted_score/query_pipeline_web_organic/2023-08-16/results/part-00876-of-00877"
            ]
        )
        | "ReadFiles" >> beam.io.ReadAllFromText(with_filename=True)
        | "ParseJSON" >> beam.Map(lambda x: (x[0], json_loads(x[1])))  # filename,
        | "Print"
        >> beam.Map(
            lambda x: print(len(x[1]))
        )  # list, length 48 or 34, each row is a request
    )


## generated tfrecords
train_tfrecord_path = "gs://training-prod-search-data-jtzn/neural_ranking/third_pass/temp/1683760757762/116460629853/nr-third-pass-20230817054325/beam-generate_8091988453207572480/examples_output_dir/train/part-00000-of-17561.tfrecord"
tft_model_path = "gs://training-prod-search-data-jtzn/neural_ranking/third_pass/models/nr-third-pass/preprocess_fit/2023-08-16"

(
    context_feature_spec,
    example_feature_spec,
    label_spec,
    sample_weight_spec,
) = _create_feature_spec(tft_model_path)

dataset_hparams = DatasetHparams(
    train_input_pattern=train_tfrecord_path,
    valid_input_pattern=train_tfrecord_path,
    train_batch_size=1024,
    valid_batch_size=1024,
    dataset_reader=tf.data.TFRecordDataset,
    convert_labels_to_binary=False,
)

dataset_builder = SimpleDatasetBuilder(
    context_feature_spec=context_feature_spec,
    example_feature_spec=example_feature_spec,
    mask_feature_name="example_list_mask",
    label_spec=label_spec,
    hparams=dataset_hparams,
    sample_weight_spec=sample_weight_spec,
)

train_data = dataset_builder.build_train_dataset()


# context feature spec (4)
# {
# 'contextualInfo[name=target].docInfo.queryInfo.queryLevelMetrics.totalPurchases_log1p_standardized': FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
# 'contextualInfo[name=user].userInfo.activeUserShipToLocations.latitude': FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
# 'contextualInfo[name=user].userInfo.activeUserShipToLocations.longitude': FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
# 'contextualInfo[name=user].userInfo.activeUserShipToLocations.zip_max_log1p_standardized': FixedLenFeature(shape=[], dtype=tf.float32, default_value=None)
# }

# label spec
# ('attributions_score', FixedLenFeature(shape=(1,), dtype=tf.float32, default_value=None))

# sample weight spec
# ('sample_weight', FixedLenFeature(shape=(1,), dtype=tf.float32, default_value=None))

# train data shapes
# (features, labels, weights) =
# (
#  {
#   -- example features
#   candidateInfo.candidateInfo.avgPropensity_numCarts:prod_nan_log1p_standardized: (1024, None),  tf.float32
#   candidateInfo.candidateInfo.avgPropensity_numClicks:prod_nan_log1p_standardized: (1024, None),  tf.float32
#   ...
#   contextualInfo[name=user].candidateInfo.countryId:is_identical: (1024, None),  tf.float32
#   contextualInfo[name=user].candidateInfo.euclidean_distance: (1024, None),  tf.float32
#   -- context features
#   contextualInfo[name=target].docInfo.queryInfo.queryLevelMetrics.totalPurchases_log1p_standardized: (1024, None),  tf.float32
#   contextualInfo[name=user].userInfo.activeUserShipToLocations.latitude: (1024, None),  tf.float32
#   contextualInfo[name=user].userInfo.activeUserShipToLocations.longitude: (1024, None),  tf.float32
#   contextualInfo[name=user].userInfo.activeUserShipToLocations.zip_max_log1p_standardized: (1024, None),  tf.float32
#   example_list_mask: (1024, None),  tf.bool
#  },
#  (1024, None),  tf.float32
#  (1024, None),  tf.float32
# )

# example_feature_spec (72)
# {
# 'candidateInfo.candidateInfo.avgPropensity_numCarts:prod_nan_log1p_standardized': FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
# 'candidateInfo.candidateInfo.avgPropensity_numClicks:prod_nan_log1p_standardized': FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
# 'candidateInfo.candidateInfo.avgPropensity_numImpressions:prod_nan_log1p_standardized': FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
# 'candidateInfo.candidateInfo.count_totalOrders:ratio_nan_log1p_standardized': FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
# 'candidateInfo.candidateInfo.domesticShipCost_priceUsd:ratio_nan_log1p_standardized': FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
# 'candidateInfo.candidateInfo.shopViews_shopImpressions:ratio_nan_log1p_standardized': FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
# 'candidateInfo.candidateInfo.smoothCartAdd_smoothCartAdd:diff_nan_log1p_standardized': FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
# 'candidateInfo.candidateInfo.smoothCartAdd_smoothCtr:ratio_nan_log1p_standardized': FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
# 'candidateInfo.candidateInfo.smoothCartAdd_smoothPurchase:ratio_nan_log1p_standardized': FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
# 'candidateInfo.candidateInfo.smoothCart_smoothCart:diff_nan_log1p_standardized': FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
# 'candidateInfo.candidateInfo.smoothPurchase_smoothCartAdd:ratio_nan_log1p_standardized': FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
# 'candidateInfo.candidateInfo.totalOrders_shopViews:ratio_nan_log1p_standardized': FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
# 'candidateInfo.candidateInfo.usShipCost_priceUsd:ratio_nan_log1p_standardized': FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
# 'candidateInfo.contextualInfo[name=target].avgClickUsdPriceDiff_avgClickPrice:ratio_log1p_standardized': FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
# 'candidateInfo.contextualInfo[name=target].avgPurchaseUsdPriceDiff_avgClickPrice:ratio_log1p_standardized': FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
# 'candidateInfo.contextualInfo[name=target].clickTopQuery_neural_ir_embedding_query_neural_ir_embedding:cosine': FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
# 'candidateInfo.contextualInfo[name=target].listing_neural_ir_embedding_query_neural_ir_embedding:cosine': FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
# 'candidateInfo.contextualInfo[name=target].priceUsd_avgClickPrice:diff_nan_log1p_standardized': FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
# 'candidateInfo.contextualInfo[name=target].priceUsd_avgClickPrice:ratio_nan_log1p_standardized': FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
# 'candidateInfo.contextualInfo[name=target].priceUsd_avgPurchasePrice:diff_nan_log1p_standardized': FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
# 'candidateInfo.contextualInfo[name=target].priceUsd_avgPurchasePrice:ratio_nan_log1p_standardized': FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
# 'candidateInfo.contextualInfo[name=target].shopName_neural_ir_embedding_query_neural_ir_embedding:cosine': FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
# 'candidateInfo.contextualInfo[name=user].originPostalCode_zip_max:abs_diff_log1p_standardized': FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
# 'candidateInfo.docInfo.listingInfo.activeListingBasics.daysSinceCreate_truncated_standardized': FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
# 'candidateInfo.docInfo.listingInfo.activeListingBasics.daysSinceOriginalCreate_truncated_standardized': FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
# 'candidateInfo.docInfo.listingInfo.activeListingBasics.gmsPercentile_standardized': FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
# 'candidateInfo.docInfo.listingInfo.activeListingBasics.imageCount_standardized': FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
# 'candidateInfo.docInfo.listingInfo.activeListingBasics.materialCount_standardized': FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
# 'candidateInfo.docInfo.listingInfo.activeListingBasics.maxPriceUsd_truncated_standardized': FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
# 'candidateInfo.docInfo.listingInfo.activeListingBasics.newListingFlag': FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
# 'candidateInfo.docInfo.listingInfo.activeListingBasics.normalizedDailyGms_log1p_standardized': FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
# 'candidateInfo.docInfo.listingInfo.activeListingBasics.normalizedDailyQuantity_log1p_standardized': FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
# 'candidateInfo.docInfo.listingInfo.activeListingBasics.pastDayGms_log1p_standardized': FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
# 'candidateInfo.docInfo.listingInfo.activeListingBasics.pastDayOrders_log1p_standardized': FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
# 'candidateInfo.docInfo.listingInfo.activeListingBasics.pastDayQuantitySold_log1p_standardized': FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
# 'candidateInfo.docInfo.listingInfo.activeListingBasics.priceUsd_log1p': FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
# 'candidateInfo.docInfo.listingInfo.activeListingBasics.priceUsd_truncated_standardized': FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
# 'candidateInfo.docInfo.listingInfo.activeListingBasics.quantity_truncated_standardized': FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
# 'candidateInfo.docInfo.listingInfo.activeListingBasics.totalFavoriteCount_log1p_standardized': FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
# 'candidateInfo.docInfo.listingInfo.activeListingBasics.totalGms_log1p_standardized': FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
# 'candidateInfo.docInfo.listingInfo.activeListingBasics.totalOrders_log1p_standardized': FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
# 'candidateInfo.docInfo.listingInfo.activeListingBasics.totalQuantitySold_log1p_standardized': FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
# 'candidateInfo.docInfo.listingInfo.activeListingBasics.totalViewCount_log1p_standardized': FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
# 'candidateInfo.docInfo.listingInfo.activeListingLocation.latitude': FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
# 'candidateInfo.docInfo.listingInfo.activeListingLocation.longitude': FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
# 'candidateInfo.docInfo.listingInfo.activeListingLocation.originPostalCode_log1p_standardized': FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
# 'candidateInfo.docInfo.listingInfo.activeListingShippingCosts.hasProcessingTime': FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
# 'candidateInfo.docInfo.listingInfo.activeListingShippingCosts.maxProcessingDays_truncated_standardized': FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
# 'candidateInfo.docInfo.listingInfo.activeListingShippingCosts.minProcessingDays_truncated_standardized': FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
# 'candidateInfo.docInfo.listingInfo.activeListingShippingCosts.shipsFreeDomestic': FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
# 'candidateInfo.docInfo.listingInfo.activeListingShippingCosts.shipsFreeToUs': FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
# 'candidateInfo.docInfo.listingInfo.activeListingShippingCosts.shipsToUs': FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
# 'candidateInfo.docInfo.listingInfo.activeListingShippingCosts.usShipCost_truncated_standardized': FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
# 'candidateInfo.docInfo.listingInfo.dwellTime.pastWeekTotalDwellMs_log1p_standardized': FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
# 'candidateInfo.docInfo.listingInfo.imageQualityScore.imageQualityScore_standardized': FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
# 'candidateInfo.docInfo.listingInfo.listingSearchVisit14d.avgPosition_standardized': FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
# 'candidateInfo.docInfo.listingInfo.listingSearchVisit14d.avgPropensity_standardized': FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
# 'candidateInfo.docInfo.listingInfo.listingSearchVisit14d.numCarts_log1p_standardized': FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
# 'candidateInfo.docInfo.listingInfo.listingSearchVisit14d.numClicks_log1p_standardized': FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
# 'candidateInfo.docInfo.listingInfo.listingSearchVisit14d.numImpressions_log1p_standardized': FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
# 'candidateInfo.docInfo.listingInfo.listingSearchVisit14d.smoothCart_standardized': FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
# 'candidateInfo.docInfo.listingInfo.listingSearchVisit14d.smoothCtr_standardized': FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
# 'candidateInfo.docInfo.shopInfo.shopAvgRatings.avgRating_log1p_standardized': FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
# 'candidateInfo.docInfo.shopInfo.verticaSellerBasics.pastYearQuantitySold_log1p_standardized': FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
# 'candidateInfo.scores.score_sigmoid': FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
# 'candidateInfo.scores.score_truncated_centered': FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
# 'candidateInfo.scores.score_truncated_standardized': FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
# 'candidateInfo.scores.score_truncated_zscore': FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
# 'contextualInfo[name=target].candidateInfo.avgOrderValue_priceScaled:ratio_log1p_standardized': FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
# 'contextualInfo[name=user].candidateInfo.avgOrderValue_priceUsd:ratio_nan_log1p_standardized': FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
# 'contextualInfo[name=user].candidateInfo.countryId:is_identical': FixedLenFeature(shape=[], dtype=tf.float32, default_value=None),
# 'contextualInfo[name=user].candidateInfo.euclidean_distance': FixedLenFeature(shape=[], dtype=tf.float32, default_value=None)
# }
