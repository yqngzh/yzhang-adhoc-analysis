# new repo, set up as README
# cd ~/development/neural_ranking/second_pass
MODEL_NAME="test"
END_DATE="2025-05-03"
EVAL_DATE="2025-05-04"
PREPROCESS_CONFIG_PATH="pipeline/configs/preprocess/sr_si.yaml|prod"
TRAIN_CONFIG_PATH="pipeline/configs/train/sr_si.yaml|prod"
EVAL_CONFIG_PATH="pipeline/configs/eval/sr.yaml|prod"
FEATURE_IMPORTANCE_CONFIG_PATH="pipeline/configs/feature_importance/sr.yaml|all"

MODEL_ROOT=f"test_data/{MODEL_NAME}/model_root"
TFT_MODEL_PATH=f"{MODEL_ROOT}/preprocess_fit/{END_DATE}"
SAVED_MODEL_PATH=f"{MODEL_ROOT}/train/{END_DATE}/main/export/single_device_model"



########    Fit    ########
import os
from functools import partial
from dataclasses import dataclass, fields
import tensorflow as tf
import apache_beam as beam
from utils import utils, schema_fns, beam_fn, preprocessing, beam_utils
from utils.preprocess.config import load_fit_config
from second_pass.data_preprocessing.preprocess_fit import preprocess

end_date = END_DATE
transform_fn_output_dir = TFT_MODEL_PATH
neural_ir_model_path = f"{END_DATE}-latest"
preprocess_config_path = PREPROCESS_CONFIG_PATH
should_collapse_features = True
is_test_run = True

config = load_fit_config(preprocess_config_path)
input_configs = config.input_configs

for field in fields(config):
    print(field.name)

dated_input_file_list = utils.get_dated_input_file_list(
    input_configs=input_configs,
    end_date=end_date,
    is_test_run=is_test_run,
    convert_loose=config.use_loose_attribution,
)
dated_input_file_list
# gs://etldata-prod-search-ranking-data-hkwv8r/tm/version=1/feature_logging_training_data_parquet/query_pipeline_web_organic/tight_purchase/_DATE=2025-05-03/results/part-00000*

max_num_of_days = max([c.num_of_days for c in input_configs])
neural_ir_model_path = utils.get_neural_ir_model_path(
    model_base_path=config.neural_ir_base_path,
    saved_model_path_prefix=config.neural_ir_saved_model_path_prefix,
    date_str_delimiter=config.neural_ir_path_date_delimiter,
    end_date=end_date,
    days_substracted=max_num_of_days - 1,  # Since `end_date` is inclusive
)
neural_ir_model_path
# gs://training-prod-search-data-jtzn/neural_ir_loc/large-voc-hard-vertex/model/2025-01-23/checkpoints/saved_model_val10

raw_feature_spec, raw_domains = config.get_tft_raw_feature_spec()
len(raw_feature_spec)
# 239 raw features
raw_defaults = config.get_raw_defaults()
raw_metadata = schema_fns.create_raw_metadata(
    raw_feature_spec=raw_feature_spec,
    raw_domains=raw_domains,
)

feature_shapes_path = os.path.join(transform_fn_output_dir, "feature_shapes.json")

preprocess_fn = partial(
    preprocess,
    neural_ir_model_path=neural_ir_model_path,
    config=config,
    feature_shapes_path=feature_shapes_path,
    should_collapse_features=should_collapse_features,
)

with beam.Pipeline() as pipeline:
    raw_data = (
        pipeline
        | "CreateFileList" >> beam.Create(dated_input_file_list)
        | "ReadFiles"
        >> beam.io.ReadAllFromParquet(
            with_filename=config.should_filter_in_preprocess_fit,
            columns=list(config.features_and_labels.keys()),
        )
        | "RemovePath" >> beam.Map(lambda x: x[1])
    )
    if is_test_run:
        raw_data = beam_utils.limit_pcollection_to_n_elements(
            pcollection=raw_data,
            n=10,
        )
    # raw_data | "print" >> beam.Map(lambda x: print(len(x[1]["candidateInfo.docInfo.listingInfo.dwellTime.pastWeekTotalDwellMs"])))
    raw_data = (
        raw_data
        | "ReplaceMissingFeatures"
        >> beam.ParDo(beam_fn.ReplaceDefaults(raw_defaults=raw_defaults))
        | "FlattenRequestsToListings"
        >> beam.FlatMap(preprocessing.flatten_batched_listings)
    )
    raw_data | "print" >> beam.Map(lambda x: print(x["candidateInfo.docInfo.listingInfo.dwellTime.pastWeekTotalDwellMs"]))


        
        
########    Generate    ######## 
# run local_runner preprocess_fit step
from utils import beam_fn, utils, beam_utils
from utils.preprocess.config import load_generate_config
from utils.preprocess.data_loading import get_ec_specs
from utils.tfdv import tfdv_utils
import tensorflow_transform as tft

transform_fn_dir = TFT_MODEL_PATH
examples_output_dir = f"{MODEL_ROOT}/output_data" 

config = load_generate_config(preprocess_config_path)
input_configs = config.input_configs

dated_input_file_list = utils.get_dated_input_file_list(
    input_configs=input_configs,
    end_date=end_date,
    convert_loose=config.use_loose_attribution,
    is_test_run=is_test_run,
)
dated_input_file_list
# gs://etldata-prod-search-ranking-data-hkwv8r/tm/version=1/feature_logging_training_data_parquet/query_pipeline_web_organic/tight_purchase/_DATE=2025-05-03/results/part-00000*

train_example_output_dir = os.path.join(examples_output_dir, "train")
# ~/test_data/test/model_root/output_data/train
valid_example_output_dir = os.path.join(examples_output_dir, "valid")
train_valid_split = [50, 50]
num_partitions = len(train_valid_split)

tft_output = tft.TFTransformOutput(transform_fn_dir)

raw_feature_spec = tft_output.raw_feature_spec()
raw_defaults = config.get_raw_defaults()

# For encoding preprocessed training data
transformed_feature_spec = tft_output.transformed_feature_spec()
# candidateInfo.collapsed.floats, shape 1750
# contextualInfo.collapsed.floats, shape 1098
example_feature_spec, context_feature_spec = get_ec_specs(transformed_feature_spec)

with beam.Pipeline() as pipeline:
    path_and_grouped_flattened_listings = (
        pipeline
        | "CreateFileList" >> beam.Create(dated_input_file_list)
        | "ReadFiles"
        >> beam.io.ReadAllFromParquet(
            with_filename=True,
            columns=list(raw_feature_spec.keys()),
        )
    )
    grouped_flattened_listings = (
        path_and_grouped_flattened_listings
        | "FilterGroupData"
        >> beam.ParDo(
            beam_fn.FilterGroupData(
                input_configs=input_configs,
                convert_loose=config.use_loose_attribution,
                min_request_length=config.min_request_length,
                remove_missing_context=config.remove_missing_context,
                remove_missing_listing_ids=config.remove_missing_listing_ids,
                remove_web_sign_out=config.remove_web_sign_out,
                remove_sign_in=config.remove_sign_in,
            )
        )
        | "RemovePath" >> beam.Map(lambda x: x[1])
    )
    if is_test_run:
        grouped_flattened_listings = beam_utils.limit_pcollection_to_n_elements(
            pcollection=grouped_flattened_listings,
            n=100,
        )
    # grouped_flattened_listings | "print" >> beam.Map(lambda x: print(x["requestUUID"]))
    encoded_dataset = (
        grouped_flattened_listings
        | "ReplaceMissingFeatures"
        >> beam.ParDo(beam_fn.ReplaceDefaults(raw_defaults=raw_defaults))
        | "UpdateReturns"
        >> beam.ParDo(
            beam_fn.UpdateReturns(
                update_return_method=config.update_return_method,
                top_n_returned_listings=config.top_n_returned_listings,
                test_run=is_test_run,
            )
        )
        | "FilterIrrelevantListingPurchases"
        >> beam.ParDo(
            beam_fn.FilterIrrelevantListingPurchases(
                remove_irrelevant_purchases=config.remove_irrelevant_purchases,
            )
        )
        | "PrepareTFTInput"
        >> beam.ParDo(
            beam_fn.PrepareTFTInput(
                raw_feature_spec=raw_feature_spec,
            )
        )
        | "AddPartitionId"
        >> beam.ParDo(
            beam_fn.AddPartitionId(
                num_partitions=num_partitions,
                train_valid_split_normalized=train_valid_split,
                partition_id_field="requestUUID",
            )
        )
        | "TransformGroupData"
        >> beam.ParDo(
            beam_fn.TransformGroupDataWithPartitionId(
                transform_fn_dir,
            )
        )
    )
    encoded_dataset | "print" >> beam.Map(lambda x: print(x[1]))




########    Train    ########
import os
import json
import tensorflow as tf
import tensorflow_ranking as tfr
from utils.train.config import load_train_config
from utils.preprocess.config import load_preprocess_config
from utils.preprocess.data_loading import create_feature_spec
from utils.train import scorer as scorer_fns
from second_pass.dnn.train import pipeline

preprocess_config_path = PREPROCESS_CONFIG_PATH
train_config_path = TRAIN_CONFIG_PATH
model_dir = f"{MODEL_ROOT}/train/{END_DATE}"
train_input_pattern = f"{MODEL_ROOT}/output_data/train/part-*.tfrecord"
valid_input_pattern = f"{MODEL_ROOT}/output_data/valid/part-*.tfrecord"
tft_model_path = TFT_MODEL_PATH
test_run=True
    
train_config = load_train_config(train_config_path)
setattr(train_config, "batch_size", 1)

preprocess_config = load_preprocess_config(preprocess_config_path)

dataset_hparams = pipeline.BasicDatasetHparams(
    train_input_pattern=train_input_pattern,
    valid_input_pattern=valid_input_pattern,
    train_batch_size=train_config.batch_size,
    valid_batch_size=train_config.batch_size,
    dataset_reader=tf.data.TFRecordDataset,
    convert_labels_to_binary=train_config.convert_labels_to_binary,
    shuffle_buffer_size=train_config.shuffle_buffer_size,
    prefetch_buffer_size=train_config.prefetch_buffer_size,
    reader_num_threads=train_config.reader_num_threads,
    num_parser_threads=train_config.num_parser_threads,
)

(
    context_feature_spec,
    example_feature_spec,
    _label_spec,
    sample_weight_spec,
) = create_feature_spec(tft_model_path, preprocess_config)

feature_shapes_path = os.path.join(tft_model_path, "feature_shapes.json")
with tf.io.gfile.GFile(feature_shapes_path) as f:
    feature_shapes = json.load(f)
        
label_spec = {}
loss_functions_dict = {}
loss_weights_dict = {}
task_layer_dims = []

train_config.task_configs
# Form dicts where key is task name and value is the corresponding label/loss/weight
for task_config in train_config.task_configs:
    label_spec[task_config.task_name] = (
        task_config.target_feature,
        _label_spec[task_config.target_feature],
    )
    loss_functions_dict[task_config.task_name] = task_config.loss
    loss_weights_dict[task_config.task_name] = float(task_config.loss_weight)
    task_layer_dims.append(task_config.task_hidden_layer_dims)

task_names = list(label_spec.keys())

pipeline_hparams = tfr.keras.pipeline.PipelineHparams(
    model_dir=model_dir,
    num_epochs=train_config.num_epochs,
    steps_per_epoch=None,
    validation_steps=None,
    loss=loss_functions_dict,
    loss_weights=loss_weights_dict,
    loss_reduction=tf.losses.Reduction.AUTO,
    optimizer=train_config.optimizer,
    learning_rate=train_config.learning_rate,
    steps_per_execution=1,
    export_best_model=train_config.export_best_model,
    best_exporter_metric_higher_better=train_config.best_exporter_metric_higher_better,
    best_exporter_metric=train_config.best_exporter_metric,
    strategy=train_config.training_strategy,
)

_MASK = "example_list_mask"


# https://github.com/tensorflow/ranking/blob/5b1d1338c1cb76507466d062e2d48bae57b17ac4/tensorflow_ranking/python/keras/pipeline.py#L561

#####  build dataset
dataset_builder=pipeline.MultiLabelBasicDatasetBuilder(
    context_feature_spec=context_feature_spec,
    example_feature_spec=example_feature_spec,
    mask_feature_name=_MASK,
    label_spec=label_spec,
    hparams=dataset_hparams,
    sample_weight_spec=sample_weight_spec
    if train_config.use_sample_weighting
    else None,
)

train_dataset = dataset_builder.build_train_dataset()
val_dataset = dataset_builder.build_valid_dataset()

# tuple of dict
# {
#     'clientProvidedInfo.browser.page': TensorSpec(shape=(1,), dtype=tf.int64, name=None), 
#     'clientProvidedInfo.browser.platform': TensorSpec(shape=(1,), dtype=tf.int64, name=None), 
#     'clientProvidedInfo.user.userCountry': TensorSpec(shape=(1,), dtype=tf.int64, name=None),
#     'contextualInfo.collapsed.floats': TensorSpec(shape=(1, 1098), dtype=tf.float32, name=None), 
#     'contextualInfo[name=target].docInfo.queryInfo.queryBuyerTaxoDresden.taxoId': TensorSpec(shape=(1,), dtype=tf.int64, name=None), 
#     'candidateInfo.collapsed.floats': TensorSpec(shape=(1, None, 1750), dtype=tf.float32, name=None), 
#     'candidateInfo.docInfo.listingInfo.activeListingBasics.taxonomyId': TensorSpec(shape=(1, None), dtype=tf.int64, name=None), 
#     'candidateInfo.docInfo.listingInfo.localeFeatures.listingCountry': TensorSpec(shape=(1, None), dtype=tf.int64, name=None), 
#     'candidateInfo.docInfo.listingInfo.verticaListings.currencyCode': TensorSpec(shape=(1, None), dtype=tf.int64, name=None), 
#     'trade_route': TensorSpec(shape=(1, None), dtype=tf.int64, name=None), 
#     'user_currency': TensorSpec(shape=(1, None), dtype=tf.int64, name=None), 
#     'example_list_mask': TensorSpec(shape=(1, None), dtype=tf.bool, name=None)
# }, 
# {
#     'purchase': TensorSpec(shape=(1, None), dtype=tf.float32, name=None), 
#     'cartadd': TensorSpec(shape=(1, None), dtype=tf.float32, name=None), 
#     'click': TensorSpec(shape=(1, None), dtype=tf.float32, name=None)
# }


#####  build scorer
embedding_input_output = {}  # {feature_name: [input_dim, output_dim]}
# add input dims
for vocab_config in preprocess_config.vocab_configs:
    embedding_input_output[vocab_config.feature_name] = [
        vocab_config.vocab_size + vocab_config.oov_size
    ]
# add output dims
for embedding_config in train_config.embedding_configs:
    if embedding_config.embedding_type == "embedding":
        embedding_input_output[embedding_config.feature_name].append(
            embedding_config.embedding_size
        )
    elif embedding_config.embedding_type == "one_hot":
        assert (
            embedding_config.embedding_size is None
        ), "one_hot should not have embedding_size"
        embedding_input_output[embedding_config.feature_name].append(-1)
embedding_input_output

unified_embedding_configs = []
for pc, tc in zip(
    preprocess_config.unified_embedding_configs,
    train_config.unified_embedding_configs,
):
    assert (
        pc.vocab_name == tc.vocab_name
    ), "vocab_name mismatch between preprocess and train configs."
    unified_embedding_configs.append(
        replace(pc, vocab_size=tc.vocab_size, embedding_dim=tc.embedding_dim)
    )
unified_embedding_configs

scorer = scorer_fns.DNNScorer(
    hidden_layer_dims=train_config.hidden_layer_dims,
    output_units=1,
    activation=tf.keras.activations.get(train_config.activation),
    final_activation=tf.keras.activations.get(train_config.final_activation),
    input_batch_norm=train_config.use_batch_norm,  # it has better performance when batch size is large (i.e. 32)
    use_batch_norm=train_config.use_batch_norm,
    batch_norm_moment=train_config.batch_norm_moment,
    dropout=train_config.dropout,
    kernel_regularizer=None,
    task_names=task_names,
    task_layer_dims=task_layer_dims,
    task_loss_chaining_method=train_config.task_loss_chaining_method,
    bias_tower_layer_dims=train_config.bias_tower_layer_dims,
    bias_tower_dropout=train_config.bias_tower_dropout,
    listing_position_features_keys=preprocess_config.listing_position_features_keys,
    platform_features_keys=preprocess_config.platform_features_keys,
    embedding_input_output=embedding_input_output,
    embedding_merging=train_config.embedding_merging,
    unified_embedding_configs=unified_embedding_configs,
    kernel_initializer=train_config.kernel_initializer,
    kernel_constraint=train_config.kernel_constraint,
    prune_collapsed_inputs=not preprocess_config.preprocess_prune,
    feature_shapes=feature_shapes,
    pruned_features=preprocess_config.pruned_features,
)


#####  build model (uses scorer)
model_builder = tfr.keras.model.ModelBuilder(
    input_creator=tfr.keras.model.FeatureSpecInputCreator(
        context_feature_spec, example_feature_spec
    ),
    preprocessor=tfr.keras.model.PreprocessorWithSpec({}),
    scorer=scorer,
    mask_feature_name=_MASK,
    name=f"{train_config.model_arch}_model",
)

model = model_builder.build()
model.summary()
# embedding layers correspond to embedding_input_output above
# __________________________________________________________________________________________________
#  Layer (type)                Output Shape                 Param #   Connected to                  
# ==================================================================================================
#  clientProvidedInfo.browser  [(None,)]                    0         []                            
#  .page (InputLayer)                                                                               
                                                                                                  
#  clientProvidedInfo.browser  [(None,)]                    0         []                            
#  .platform (InputLayer)                                                                           
                                                                                                  
#  clientProvidedInfo.user.us  [(None,)]                    0         []                            
#  erCountry (InputLayer)                                                                           
                                                                                                  
#  contextualInfo.collapsed.f  [(None, 1098)]               0         []                            
#  loats (InputLayer)                                                                               
                                                                                                  
#  contextualInfo[name=target  [(None,)]                    0         []                            
#  ].docInfo.queryInfo.queryB                                                                       
#  uyerTaxoDresden.taxoId (In                                                                       
#  putLayer)                                                                                        
                                                                                                  
#  candidateInfo.collapsed.fl  [(None, None, 1750)]         0         []                            
#  oats (InputLayer)                                                                                
                                                                                                  
#  candidateInfo.docInfo.list  [(None, None)]               0         []                            
#  ingInfo.activeListingBasic                                                                       
#  s.taxonomyId (InputLayer)                                                                        
                                                                                                  
#  candidateInfo.docInfo.list  [(None, None)]               0         []                            
#  ingInfo.localeFeatures.lis                                                                       
#  tingCountry (InputLayer)                                                                         
                                                                                                  
#  candidateInfo.docInfo.list  [(None, None)]               0         []                            
#  ingInfo.verticaListings.cu                                                                       
#  rrencyCode (InputLayer)                                                                          
                                                                                                  
#  trade_route (InputLayer)    [(None, None)]               0         []                            
                                                                                                  
#  user_currency (InputLayer)  [(None, None)]               0         []                            
                                                                                                  
#  example_list_mask (InputLa  [(None, None)]               0         []                            
#  yer)                                                                                             
                                                                                                  
#  flatten_list_1 (FlattenLis  ({'clientProvidedInfo.user   0         ['clientProvidedInfo.browser.p
#  t)                          .userCountry': (None,),                age[0][0]',                   
#                               'contextualInfo.collapsed              'clientProvidedInfo.browser.p
#                              .floats': (None, 1098),                latform[0][0]',               
#                               'contextualInfo[name=targ              'clientProvidedInfo.user.user
#                              et].docInfo.queryInfo.quer             Country[0][0]',               
#                              yBuyerTaxoDresden.taxoId':              'contextualInfo.collapsed.flo
#                               (None,),                              ats[0][0]',                   
#                               'clientProvidedInfo.brows              'contextualInfo[name=target].
#                              er.page': (None,),                     docInfo.queryInfo.queryBuyerTa
#                               'clientProvidedInfo.brows             xoDresden.taxoId[0][0]',      
#                              er.platform': (None,)},                 'candidateInfo.collapsed.floa
#                               {'candidateInfo.collapsed             ts[0][0]',                    
#                              .floats': (None, 1750),                 'candidateInfo.docInfo.listin
#                               'candidateInfo.docInfo.li             gInfo.activeListingBasics.taxo
#                              stingInfo.activeListingBas             nomyId[0][0]',                
#                              ics.taxonomyId': (None,),               'candidateInfo.docInfo.listin
#                               'candidateInfo.docInfo.li             gInfo.localeFeatures.listingCo
#                              stingInfo.localeFeatures.l             untry[0][0]',                 
#                              istingCountry': (None,),                'candidateInfo.docInfo.listin
#                               'candidateInfo.docInfo.li             gInfo.verticaListings.currency
#                              stingInfo.verticaListings.             Code[0][0]',                  
#                              currencyCode': (None,),                 'trade_route[0][0]',         
#                               'trade_route': (None,),                'user_currency[0][0]',       
#                               'user_currency': (None,)}              'example_list_mask[0][0]']   
#                              )                                                                    
                                                                                                  
#  flatten_11 (Flatten)        (None, 1098)                 0         ['flatten_list_1[0][3]']      
                                                                                                  
#  flatten_12 (Flatten)        (None, 1750)                 0         ['flatten_list_1[0][5]']      
                                                                                                  
#  embedding (Embedding)       (None, 8)                    1616      ['flatten_list_1[0][2]']      
                                                                                                  
#  embedding_1 (Embedding)     (None, 8)                    1616      ['flatten_list_1[0][7]']      
                                                                                                  
#  embedding_2 (Embedding)     (None, 32)                   96160     ['flatten_list_1[0][9]']      
                                                                                                  
#  embedding_3 (Embedding)     (None, 2)                    6         ['flatten_list_1[0][0]']      
                                                                                                  
#  embedding_4 (Embedding)     (None, 4)                    24        ['flatten_list_1[0][1]']      
                                                                                                  
#  embedding_5 (Embedding)     (None, 8)                    1616      ['flatten_list_1[0][10]',     
#                                                                      'flatten_list_1[0][8]']      
                                                                                                  
#  embedding_6 (Embedding)     (None, 32)                   96160     ['flatten_list_1[0][6]',      
#                                                                      'flatten_list_1[0][4]']      
                                                                                                  
#  tf.concat_1 (TFOpLambda)    (None, 2982)                 0         ['flatten_11[0][0]',          
#                                                                      'flatten_12[0][0]',          
#                                                                      'embedding[0][0]',           
#                                                                      'embedding_1[0][0]',         
#                                                                      'embedding_2[0][0]',         
#                                                                      'embedding_3[0][0]',         
#                                                                      'embedding_4[0][0]',         
#                                                                      'embedding_5[0][0]',         
#                                                                      'embedding_5[1][0]',         
#                                                                      'embedding_6[0][0]',         
#                                                                      'embedding_6[1][0]']         
                                                                                                  
#  model (Functional)          [(None, 1),                  9156251   ['tf.concat_1[0][0]']         
#                               (None, 1),                                                          
#                               (None, 1)]                                                          
                                                                                                  
#  cartadd (RestoreList)       (None, None)                 0         ['model[0][1]',               
#                                                                      'example_list_mask[0][0]']   
                                                                                                  
#  click (RestoreList)         (None, None)                 0         ['model[0][2]',               
#                                                                      'example_list_mask[0][0]']   
                                                                                                  
#  purchase (RestoreList)      (None, None)                 0         ['model[0][0]',               
#                                                                      'example_list_mask[0][0]']   
                                                                                                  
# ==================================================================================================
# Total params: 9353449 (35.68 MB)
# Trainable params: 9338781 (35.62 MB)
# Non-trainable params: 14668 (57.30 KB)
# __________________________________________________________________________________________________

model.compile(
    optimizer=self._optimizer,
    loss=self.build_loss(),
    metrics=self.build_metrics(),
    loss_weights=self._hparams.loss_weights,
    weighted_metrics=(self.build_weighted_metrics()
                    if self._hparams.use_weighted_metrics else None),
    steps_per_execution=self._hparams.steps_per_execution
)

model.fit(
    x=train_dataset,
    epochs=self._hparams.num_epochs,
    steps_per_epoch=self._hparams.steps_per_epoch,
    validation_steps=self._hparams.validation_steps,
    validation_data=valid_dataset,
    callbacks=self.build_callbacks(),
    verbose=verbose
)




########    Eval    ########
from datetime import datetime, timedelta
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from utils.preprocess.config import load_preprocess_config
from utils.eval.config import load_eval_config
from utils import utils, beam_fn, beam_utils
from utils.eval import eval_utils

GCLOUD_PROJECT = "etsy-search-ml-dev"
date = EVAL_DATE
bigquery_model_name = MODEL_NAME
preprocess_config_path = PREPROCESS_CONFIG_PATH
eval_config_path = EVAL_CONFIG_PATH
saved_model_path = SAVED_MODEL_PATH
bigquery_project_id = GCLOUD_PROJECT
bigquery_output_table = f"{GCLOUD_PROJECT}.search_ranking.second_pass_eval"
test_run = True

config = load_preprocess_config(preprocess_config_path)
eval_config = load_eval_config(eval_config_path)
eval_data_configs = eval_config.eval_data
max_num_of_days = max([c.num_of_days for c in eval_data_configs])
eval_end_date = (
    datetime.strptime(date, "%Y-%m-%d") + (timedelta(days=max_num_of_days - 1))
).strftime("%Y-%m-%d")

dated_input_file_list = utils.get_dated_input_file_list(
    input_configs=eval_data_configs,
    end_date=eval_end_date,
    is_test_run=test_run,
)
dated_input_file_list

raw_feature_spec = config.features_and_labels
raw_defaults = config.get_raw_defaults()
# Update raw_defaults with eval metric dependencies if they don't exist already
for k, v in eval_utils.EVAL_METRIC_DEPENDENCY_DEFAULTS.items():
    if k not in raw_defaults:
        raw_defaults[k] = v
        
columns = list(
    set(
        list(raw_feature_spec.keys())
        + list(eval_utils.EVAL_METRIC_DEPENDENCY_DEFAULTS.keys())
        + [p.feature_name for p in eval_config.partitions]
        + [lp.feature_name for lp in eval_config.listing_partitions]
        + [f for cp in eval_config.custom_partitions for f in cp.feature_names]
    )
)        
# 241
train_config = None
score_listings_do_fn = eval_utils.ScoreListings(
    saved_model_path=saved_model_path,
    raw_feature_spec=raw_feature_spec,
    task_configs=train_config.task_configs if train_config else None,
)

with beam.Pipeline() as pipeline:
    path_and_grouped_flattened_listings = (
        pipeline
        | "CreateFileList" >> beam.Create(dated_input_file_list)
        | "ReadFiles"
        >> beam.io.ReadAllFromParquet(
            columns=columns,
            with_filename=True,
        )
    )
    if test_run:
        path_and_grouped_flattened_listings = (
            beam_utils.limit_pcollection_to_n_elements(
                pcollection=path_and_grouped_flattened_listings,
                n=10,
            )
        )
    examples = (
        path_and_grouped_flattened_listings
        | "CreateEvalData"
        >> beam.Map(
            lambda x: eval_utils.EvalData(
                path=x[0],
                flattened_features=x[1],
            )
        )
        | "UpdateReturns"
        >> beam.ParDo(
            eval_utils.UpdateReturnsWithEvalData(
                update_return_method=beam_fn.UpdateReturnMethod.NEW_LABEL,
                test_run=test_run,
            )
        )
        | "AddPartitionTags"
        >> beam.ParDo(
            eval_utils.PartitionTagsDoFn(
                eval_data_configs=eval_config.eval_data,
                partitions=eval_config.partitions,
                listing_partitions=eval_config.listing_partitions,
                custom_partitions=eval_config.custom_partitions,
            )
        )
        | "ReplaceMissingFeatures"
        >> beam.ParDo(
            eval_utils.ReplaceDefaultsWithEvalData(
                raw_defaults=raw_defaults,
            )
        )
        | "AddTFTInput"
        >> beam.ParDo(
            eval_utils.PrepareTFTInputWithEvalData(
                raw_feature_spec=raw_feature_spec,
            )
        )
    )
    examples | "print" >> beam.Map(lambda x: print(x.listing_ids))

    # predictions = examples | "ScoreListings" >> beam.ParDo(score_listings_do_fn)

