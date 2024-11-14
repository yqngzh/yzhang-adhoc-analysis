"""
This module re-implements a subset of NIR's modeling code: https://github.com/etsy/neural_ir/blob/main/neural_ir/neural_ir/models.py

It is designed to work with NIR models whose config settings are exactly `_REQUIRED_NIR_CONFIG`. Because NIR config
is backwards compatible, any model which matches this configuration will be consistent with the modeling code implemented below.

Currently, _REQUIRED_NIR_CONFIG matches NIR+T+HQI, the production web model for NIR as of 7/24/2024.

The reason we re-implement rather than simply using signatures is due to flexibility... this allows us for example
to tell NIR to ignore HQI features (which it wouldn't do by default), or to freeze specific layers while fine tuning
others
"""

import os
import tempfile
from typing import Dict, List, Literal, Optional, TypedDict

import tensorflow as tf
import tensorflow_text as text
import tensorflow_transform as tft
from transformers import TFAutoModel

from semantic_relevance.utils import paths

EntityType = Literal["query", "listing"]
FeatureType = Literal["TOKEN_IDS", "RAW_TEXT"]
NIR_HQI_FEATURES = ("clickTopQuery", "cartTopQuery", "purchaseTopQuery")


class RawListingFeatures(TypedDict):
    title: tf.Tensor  # 1-d string tensor
    tags: tf.Tensor  # 1-d string tensor
    taxonomyPath: tf.Tensor  # 1-d string tensor
    clickTopQuery: tf.RaggedTensor | None  # 2-d, n_examples by n_queries_per_listings
    cartTopQuery: tf.RaggedTensor | None
    purchaseTopQuery: tf.RaggedTensor | None


NIR_LISTING_FEATURES = frozenset(RawListingFeatures.__annotations__)


_COMMON_SP_BERT_PADDING_ID = 0
_SP_TOKENIZER = "sp_tokenizer"
_REQUIRED_NIR_CONFIG = {
    "embedding_dim": 256,
    "embedding_vocab_size": 988000,
    "features": [
        {"name": "query_char_3grams", "source_field": "query", "feature_type": "TOKEN_IDS"},
        {"name": "query_word_1grams", "source_field": "query", "feature_type": "TOKEN_IDS"},
        {"name": "query_word_2grams", "source_field": "query", "feature_type": "TOKEN_IDS"},
        {"name": "title_char_3grams", "source_field": "title", "feature_type": "TOKEN_IDS"},
        {"name": "title_word_1grams", "source_field": "title", "feature_type": "TOKEN_IDS"},
        {"name": "title_word_2grams", "source_field": "title", "feature_type": "TOKEN_IDS"},
        {"name": "tags_char_3grams", "source_field": "tags", "feature_type": "TOKEN_IDS"},
        {"name": "tags_word_1grams", "source_field": "tags", "feature_type": "TOKEN_IDS"},
        {"name": "tags_word_2grams", "source_field": "tags", "feature_type": "TOKEN_IDS"},
        {
            "name": "clickTopQuery_char_3grams",
            "source_field": "clickTopQuery",
            "feature_type": "TOKEN_IDS",
        },
        {
            "name": "clickTopQuery_word_1grams",
            "source_field": "clickTopQuery",
            "feature_type": "TOKEN_IDS",
        },
        {
            "name": "clickTopQuery_word_2grams",
            "source_field": "clickTopQuery",
            "feature_type": "TOKEN_IDS",
        },
        {
            "name": "cartTopQuery_char_3grams",
            "source_field": "cartTopQuery",
            "feature_type": "TOKEN_IDS",
        },
        {
            "name": "cartTopQuery_word_1grams",
            "source_field": "cartTopQuery",
            "feature_type": "TOKEN_IDS",
        },
        {
            "name": "cartTopQuery_word_2grams",
            "source_field": "cartTopQuery",
            "feature_type": "TOKEN_IDS",
        },
        {
            "name": "purchaseTopQuery_char_3grams",
            "source_field": "purchaseTopQuery",
            "feature_type": "TOKEN_IDS",
        },
        {
            "name": "purchaseTopQuery_word_1grams",
            "source_field": "purchaseTopQuery",
            "feature_type": "TOKEN_IDS",
        },
        {
            "name": "purchaseTopQuery_word_2grams",
            "source_field": "purchaseTopQuery",
            "feature_type": "TOKEN_IDS",
        },
        {"name": "taxonomyPath", "source_field": "taxonomyPath", "feature_type": "TOKEN_IDS"},
        {"name": "title", "source_field": "title", "feature_type": "RAW_TEXT"},
    ],
    "metric_space": "COSINE",
    "norm_layer_type": "LAYER_NORM",
    "hidden_layer_sizes": [8192, 256],
    "hidden_layer_doc_only": False,
    "hidden_layer_activation": "relu",
    "share_hidden_layer_params": False,
    "token_composition_grouping": "TAXO_TEXT_HQI_OTHER",
    "token_composition_method": "CONCAT",
    "tfhub_model_urls": [],
    "shop_embedding_dim": None,
    "shop_embedding_vocab_size": None,
    "tokenfeat2dropout": {},
    "no_learning_token_groups": [],
    "transformer_config": {
        "base_model_name": "gs://etldata-prod-search-ranking-data-hkwv8r/t-gen/saved_models/t5-small/",
        "tokenizer_name": "sp_tokenizer",
        "tokenizer_files_path": None,
        "output_dim": None,
        "max_listing_seq_len": 48,
        "max_query_seq_len": 12,
        "trainable": True,
        "query_transformer": False,
        "listing_transformer": True,
        "share_base_model": False,
        "from_pt": False,
        "final_layer_dropout": 0.1,
        "pooling_type": "mean",
        "only_embeddings": False,
        "concat_raw_text": False,
    },
}


class AverageEmbeddingLayer(tf.keras.layers.Layer):
    """This layer forms a [num_embeddings x embedding_dim] trainable embedding matrix.
    At activation time using a Tensor or SparseTensor of size N x M (where N is
    typically batch_size) M embeddings columns are picked from the weights matrix
    and are average together to form a [N x embedding_dim] averaged embeddings
    """

    def __init__(self, num_embeddings: int, embedding_dim: int, **kwargs):
        super(AverageEmbeddingLayer, self).__init__(**kwargs)
        self.num_embeddings = num_embeddings
        self.embedding_dim = embedding_dim
        self.embedding = tf.keras.layers.Embedding(num_embeddings, embedding_dim)
        self._supports_ragged_inputs = True

    @staticmethod
    def average_ragged_embeddings_per_instance(embeddings: tf.RaggedTensor) -> tf.Tensor:
        """
        :param embeddings: A ragged tensor of dimension BATCH_SIZE X (nested dims) X EMBEDDING_DIM,
            corresponding to embeddings for each instance, for each token/text instance
        :return: BATCH_SIZE X (all but last nested dim) X EMBEDDING_DIM tf.Tensor, the average embedding for each instance
            across tokens/text instances
        """

        # embeddings.ragged_rank corresponds to the last nested dim before the embeddings
        #  which we will average out and eliminate
        average = tf.reduce_mean(embeddings, axis=embeddings.ragged_rank)
        # In the case where there are zero tokens, zero out the embeddings
        average = tf.where(tf.math.is_nan(average), tf.zeros_like(average), average)
        return average

    def call(self, inputs: tf.RaggedTensor):
        looked_up_embeddings = self.embedding(inputs)
        return self.average_ragged_embeddings_per_instance(looked_up_embeddings)


class TransformerFeaturizer(tf.keras.Model):
    def __init__(
        self,
    ):
        super(TransformerFeaturizer, self).__init__()

        transformer_config: Dict = _REQUIRED_NIR_CONFIG["transformer_config"]
        self.max_seq_len = transformer_config["max_listing_seq_len"]

        self.only_embeddings = transformer_config["only_embeddings"]
        self.concat_raw_text = transformer_config["concat_raw_text"]

        base_model_name = transformer_config["base_model_name"]

        assert "gs://" in base_model_name, "Only support GCS base models"
        # Assume that `base_model_name` is a GCS path to a saved huggingface model
        with tempfile.TemporaryDirectory() as tempdir:
            paths.copy_gcs_path_recursive(base_model_name, dest_path=tempdir)
            base_model = TFAutoModel.from_pretrained(tempdir)

        # NOTE: this line appears useless, but is needed to make keras model serialization work
        # when we only use a sub-part of the model
        # (otherwise it complains about untracked resources)
        self.full_base_model = base_model
        assert "t5" in base_model_name, "Only support T5 models"
        # T5 is encoder/decoder, we just use the encoder
        self.base_model = base_model.encoder

        self.base_model.trainable = transformer_config["trainable"]
        self.dropout = tf.keras.layers.Dropout(rate=transformer_config["final_layer_dropout"])
        self.pooler = None
        self.pooling_type = transformer_config["pooling_type"]
        if transformer_config["output_dim"] is not None:
            raise ValueError("Not supported with these NIR settings")

    def call(self, tokens: tf.RaggedTensor, training=False):
        """
        :param tokens: A tf.RaggedTensor with tokens, no padding
        :return:
        """
        input_ids = tokens[:, : self.max_seq_len].to_tensor(_COMMON_SP_BERT_PADDING_ID)
        input_mask = tf.cast(input_ids != _COMMON_SP_BERT_PADDING_ID, input_ids.dtype)

        assert not self.only_embeddings, "Don't support embedding-only mode"
        bert_output = self.base_model.call(input_ids=input_ids, attention_mask=input_mask)
        last_hidden_state = bert_output.last_hidden_state

        assert self.pooling_type == "mean", "Only support mean pooling"
        ragged_input_mask = tf.RaggedTensor.from_tensor(input_mask)
        masked_last_hidden_state = tf.ragged.boolean_mask(
            last_hidden_state, tf.equal(ragged_input_mask, 1)
        )
        embedding = tf.reduce_mean(masked_last_hidden_state, axis=1)

        embedding = self.dropout(embedding, training=training)
        if self.pooler is not None:
            embedding = self.pooler(embedding)

        return embedding


def create_transformer_tokenizer():
    transformer_config: Dict = _REQUIRED_NIR_CONFIG["transformer_config"]
    assert (
        transformer_config["tokenizer_name"] == _SP_TOKENIZER
    ), "Only support sentencepiece tokenizer"
    sp_model_path = "gs://etldata-prod-search-ranking-data-hkwv8r/user/rjha/query-rewriting/saved_model_files/spiece.model"

    model_name = "spiece.model"
    with tempfile.TemporaryDirectory() as tempdir:
        local_model_path = os.path.join(tempdir, model_name)
        tf.io.gfile.copy(sp_model_path, local_model_path)
        spiece_model = open(local_model_path, "rb").read()
    return text.SentencepieceTokenizer(model=spiece_model, name="sp_tokenizer", add_eos=True)


class NirModelWrapper(tf.keras.Model):
    def __init__(self):
        super(NirModelWrapper, self).__init__()
        self.hidden_layer_sizes = _REQUIRED_NIR_CONFIG["hidden_layer_sizes"]

        self.token_embeddings = AverageEmbeddingLayer(
            _REQUIRED_NIR_CONFIG["embedding_vocab_size"], _REQUIRED_NIR_CONFIG["embedding_dim"]
        )

        def create_norm_layer(name):
            return tf.keras.layers.LayerNormalization(name=name)

        self.query_embedding_norm_layer = create_norm_layer("query_norm")
        self.doc_embedding_norm_layer = create_norm_layer("doc_norm")

        def create_dense_layers():
            return [
                tf.keras.layers.Dense(s, _REQUIRED_NIR_CONFIG["hidden_layer_activation"])
                for s in self.hidden_layer_sizes
            ]

        self.doc_dense_layers = create_dense_layers()
        self.query_dense_layers = create_dense_layers()
        self.doc_dense_norm_layers = [
            create_norm_layer(f"doc_norm_{i}") for i, _ in enumerate(self.doc_dense_layers)
        ]
        self.query_dense_norm_layers = [
            create_norm_layer(f"query_norm_{i}") for i, _ in enumerate(self.query_dense_layers)
        ]

        self.listing_transformer = TransformerFeaturizer()
        self.query_transformer = None
        self.transformer_tokenizer = create_transformer_tokenizer()

    def _get_embedding(
        self,
        inputs: Dict,
        entity_type: EntityType,
        input_norm_layer: tf.keras.layers.Layer,
        dense_layers: List[tf.keras.layers.Dense],
        dense_norm_layers: List[tf.keras.layers.Layer],
        training=None,
        normalize=False,
        transformer_featurizer: Optional["TransformerFeaturizer"] = None,
        use_hqi=True,
    ):
        embs_dict = {}
        # Get one embedding per group, each of dimension batch_size X emb_dim
        group_token_embeddings = []
        token_types = ["word_1grams", "word_2grams", "char_3grams"]
        if entity_type == "query":
            token_id_tensor_groups = {"text": [inputs[f"query_{tt}"] for tt in token_types]}
            raw_text_feature_names = []
        elif entity_type == "listing":
            token_id_tensor_groups = {
                "taxo": [inputs["taxonomyPath"]],
                "text": [
                    inputs[f"{feat}_{tt}"] for feat in ["tags", "title"] for tt in token_types
                ],
                "hqi": [
                    inputs[f"{int}TopQuery_{tt}"]
                    for int in ["click", "cart", "purchase"]
                    for tt in token_types
                ],
            }
            raw_text_feature_names = ["title"]
        else:
            raise ValueError(f"Invalid entity type {entity_type}")

        for group_name in sorted(token_id_tensor_groups):
            if not use_hqi and group_name == "hqi":
                n_rows = tf.shape(inputs["title"])[0]
                avg_token_embeddings = tf.zeros([n_rows, self.token_embeddings.embedding_dim])
            else:
                token_tensors = token_id_tensor_groups[group_name]
                full_token_id_tensor = tf.concat(token_tensors, axis=1)
                avg_token_embeddings = self.token_embeddings(full_token_id_tensor)
                avg_token_embeddings = tf.ensure_shape(avg_token_embeddings, (None, None))

            group_token_embeddings.append(avg_token_embeddings)

        x = None

        if len(group_token_embeddings) > 0:
            final_tokens_embedding = tf.concat(group_token_embeddings, axis=1)
            x = final_tokens_embedding

        if transformer_featurizer is not None and len(raw_text_feature_names) > 0:
            if len(raw_text_feature_names) != 1:
                raise ValueError("For now, don't support more than 1 raw text feature")
            text_feature_name = raw_text_feature_names[0]
            text_tokens_feature_name = f"{text_feature_name}_tokens"

            if text_tokens_feature_name in inputs:
                transformer_input = inputs[text_tokens_feature_name]
            else:
                raw_text = tf.squeeze(inputs[text_feature_name])
                raw_text = tf.ensure_shape(raw_text, [None])
                transformer_input = self.transformer_tokenizer.tokenize(raw_text)
            embedded_text_tensor = transformer_featurizer(transformer_input, training=training)

            if x is None:
                x = embedded_text_tensor
            else:
                x = tf.concat([x, embedded_text_tensor], axis=1)

        x = input_norm_layer(x, training=training)

        for i, (dense_layer, norm_layer) in enumerate(zip(dense_layers, dense_norm_layers)):
            x = dense_layer(x)
            x = norm_layer(x, training=training)
            embs_dict[f"hidden_{i}"] = x

        post_activation = tf.nn.tanh(x)
        embs_dict["pre_norm"] = post_activation
        if normalize:
            post_activation = tf.math.l2_normalize(post_activation, axis=1)

        embs_dict["final"] = post_activation
        return embs_dict

    def get_query_embedding(self, inputs: Dict, training=None, normalize=True):
        return self._get_embedding(
            inputs,
            "query",
            self.query_embedding_norm_layer,
            self.query_dense_layers,
            self.query_dense_norm_layers,
            training=training,
            normalize=normalize,
            transformer_featurizer=self.query_transformer,
        )

    def get_doc_embedding(self, inputs: Dict, training=None, normalize=True, use_hqi=True):
        return self._get_embedding(
            inputs,
            "listing",
            self.doc_embedding_norm_layer,
            self.doc_dense_layers,
            self.doc_dense_norm_layers,
            training=training,
            normalize=normalize,
            transformer_featurizer=self.listing_transformer,
            use_hqi=use_hqi,
        )

    @classmethod
    def from_config(cls, config_dict: Dict, **kwargs):
        assert (
            config_dict == _REQUIRED_NIR_CONFIG
        ), "Config must exactly conform to _REQUIRED_NIR_CONFIG"
        model = NirModelWrapper()
        return model

    @staticmethod
    def load_from_checkpoint(checkpoint_path):
        return tf.keras.models.load_model(
            checkpoint_path,
            compile=False,
            options=tf.saved_model.LoadOptions(allow_partial_checkpoint=True),
            custom_objects={
                "SemanticProductSearch": NirModelWrapper,
                "AverageEmbeddingLayer": AverageEmbeddingLayer,
                "TransformerFeaturizer": TransformerFeaturizer,
            },
        )
        # not supported in API in tf 2.17

    def call(self, x, training=False):
        # This dummy method is added only because when this instance is attached to other models,
        # it is needed for saving
        return x


class NirWrapper(tf.keras.layers.Layer):
    """
    This wraps both the NIR model itself, and NIR preprocessing from NIR prod models
    """

    def __init__(
        self,
        nir_model: NirModelWrapper,
        nir_tft: tf.keras.layers.Layer,
        nir_model_path,
        nir_tft_path,
    ):
        super().__init__()
        self.nir_model = nir_model
        self.nir_tft = nir_tft
        self.nir_model_path = nir_model_path
        self.nir_tft_path = nir_tft_path

    @staticmethod
    def from_model_paths(saved_model_path: str, tft_model_path: str):
        nir_model = NirModelWrapper.load_from_checkpoint(saved_model_path)
        nir_tft_layer = tft.TFTransformOutput(tft_model_path).transform_features_layer()
        return NirWrapper(
            nir_model, nir_tft_layer, nir_model_path=saved_model_path, nir_tft_path=tft_model_path
        )

    @staticmethod
    def _sparse_to_ragged(tensor_dict):
        new_dict = dict(tensor_dict)
        for k, v in tensor_dict.items():
            if isinstance(v, tf.SparseTensor):
                new_dict[k] = tf.RaggedTensor.from_sparse(v)
        return new_dict

    def preproc_query(self, query: tf.Tensor) -> Dict[str, tf.Tensor]:
        """
        :param query: 1-d string tensor
        :return: Dict tensor of preprocessed query features, suitable for input to query tower
        """
        return self._sparse_to_ragged(self.nir_tft({"query": query}))

    def embed_query_features(self, query_features: Dict[str, tf.Tensor], training=False):
        emb = self.nir_model.get_query_embedding(query_features, training=training)
        return emb

    def embed_shop_features_as_query(self, shop_features: Dict[str, tf.Tensor], training=False):
        query_features = {
            f"query_{postfix}": shop_features[f"shop_{postfix}"]
            for postfix in ("word_1grams", "word_2grams", "char_3grams")
        }
        emb = self.nir_model.get_query_embedding(query_features, training=training)
        return emb

    def embed_title_features_as_query(self, title_features: Dict[str, tf.Tensor], training=False):
        query_features = {
            f"query_{postfix}": title_features[f"title_{postfix}"]
            for postfix in ("word_1grams", "word_2grams", "char_3grams")
        }
        emb = self.nir_model.get_query_embedding(query_features, training=training)
        return emb

    def embed_query(self, query: tf.Tensor):
        model_input = self.preproc_query(query)
        emb = self.nir_model.get_query_embedding(model_input)
        return emb

    def preproc_listing(self, raw_listing_features: RawListingFeatures):
        tft_listing_features = dict(raw_listing_features)

        # If hqi queries are not passed in, impute them as empty
        title = raw_listing_features["title"]
        for hqi_feature in ("clickTopQuery", "cartTopQuery", "purchaseTopQuery"):
            hqi_value: tf.RaggedTensor = raw_listing_features.get(hqi_feature, None)
            if hqi_value is None:
                n_rows = tf.shape(title)[0]
                empty_hqi = tf.zeros([n_rows, 0], tf.string)
                hqi_value = tf.RaggedTensor.from_tensor(empty_hqi)

            hqi_value = hqi_value.to_sparse()
            tft_listing_features[hqi_feature] = hqi_value

        tft_features = self._sparse_to_ragged(self.nir_tft(tft_listing_features))
        title_transformer_tokens = self.nir_model.transformer_tokenizer.tokenize(title)

        all_features = {**tft_features, "title_tokens": title_transformer_tokens}
        return all_features

    def embed_listing_features(
        self, listing_features: Dict[str, tf.Tensor], training=False, use_hqi=True
    ):
        emb = self.nir_model.get_doc_embedding(listing_features, training=training, use_hqi=use_hqi)
        return emb

    def embed_raw_listing(self, raw_listing_features: RawListingFeatures):
        model_input = self.preproc_listing(raw_listing_features)
        emb = self.nir_model.get_doc_embedding(model_input)
        return emb