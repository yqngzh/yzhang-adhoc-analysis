import click
from typing import Literal
import os
from datetime import timedelta
import pandas as pd
import tensorflow as tf

from semantic_relevance.training.train_semrel_filter import TrainingLabel, SemrelModel
from semantic_relevance.utils.nir import get_latest_nir_metadata
from semantic_relevance.training.tensorflow_models.nirt_prod_model import NirWrapper
from semantic_relevance.utils.configs import load_model_dev_config
from semantic_relevance.utils.data_loading import load_data_list, combine_dfs
from semantic_relevance.utils.logs import logger
from semantic_relevance.utils.constants import (
    QUERY_KEY,
    LISTING_ID_KEY,
    LISTING_TITLE_KEY,
    LISTING_DESCRIPTION_KEY,
    LISTING_DESCRIPTION_KEYWORDS_KEY,
    LISTING_TAXO_KEY,
    LISTING_TAGS_KEY,
    LISTING_ATTRIBUTES_CLEAN_KEY,
    SHOP_NAME_KEY,
    LABEL_KEY,
    LABEL_PROBA_KEY,
)


V2_LISTING_STRING_COLUMNS = [
    LISTING_TITLE_KEY,
    LISTING_DESCRIPTION_KEY,
    LISTING_DESCRIPTION_KEYWORDS_KEY,
    LISTING_TAXO_KEY,
    LISTING_TAGS_KEY,
    LISTING_ATTRIBUTES_CLEAN_KEY,
    SHOP_NAME_KEY,
]
V2_SERVING_DF_COLUMNS = (
    [QUERY_KEY, LISTING_ID_KEY] + V2_LISTING_STRING_COLUMNS + [LABEL_KEY, LABEL_PROBA_KEY]
)


class SemrelModelServingV2(SemrelModel):
    def __init__(
        self,
        nir_wrapper: NirWrapper,
        n_classes,
        hidden_sizes=(256,),
        activation="relu",
        call_fn_use_logits=True,
        nir_emb_key="final",
        nir_emb_proj=None,
        nir_emb_fusion="lth",
    ):
        super().__init__(
            nir_wrapper=nir_wrapper,
            n_classes=n_classes,
            hidden_sizes=hidden_sizes,
            activation=activation,
            call_fn_use_logits=call_fn_use_logits,
            nir_emb_key=nir_emb_key,
            nir_emb_proj=nir_emb_proj,
            nir_emb_fusion=nir_emb_fusion,
        )

        if self.use_nir_emb_proj:

            def create_proj_layer():
                return tf.keras.Sequential(
                    [
                        tf.keras.layers.Dense(nir_emb_proj, activation=activation),
                        tf.keras.layers.LayerNormalization(),
                    ]
                )

            self.nir_proj_layers: Dict | None = {
                k: create_proj_layer() for k in ("query", "listing", "title", "shop")
            }
        else:
            self.nir_proj_layers = None

    def score_query_listing(
        self,
        query_emb: tf.Tensor,
        listing_emb: tf.Tensor,
        title_emb: tf.Tensor,
        shop_emb: tf.Tensor,
        output_logits: bool,
    ):
        hadamard = query_emb * listing_emb
        hadamard_title = query_emb * title_emb
        hadamard_shop = query_emb * shop_emb

        if self.nir_emb_fusion == "lth":
            # Use both the hadamard product of <query,listing>
            # and <query, title>
            mlp_input = tf.concat([hadamard_title, hadamard], axis=1)
        elif self.nir_emb_fusion == "lh":
            # Use the hadamard product of <query,listing>
            mlp_input = hadamard
        elif self.nir_emb_fusion == "tslh":
            mlp_input = tf.concat([hadamard_title, hadamard_shop, hadamard], axis=1)
        elif self.nir_emb_fusion == "alibaba":
            # Use embedding fusion similar to described in Alibaba's ReprBERT, section 3.2:
            # https://drive.google.com/file/d/1hvxPNZ1zx9H6TJZjHnnXP1kA--uI4yGX/view?usp=sharing
            mlp_input = tf.concat(
                [
                    tf.maximum(query_emb, listing_emb),
                    query_emb - listing_emb,
                    query_emb + listing_emb,
                    tf.maximum(query_emb, title_emb),
                    query_emb - title_emb,
                    query_emb + title_emb,
                    tf.maximum(query_emb, shop_emb),
                    query_emb - shop_emb,
                    query_emb + shop_emb,
                ],
                axis=1,
            )
        elif self.nir_emb_fusion == "qlt":
            mlp_input = tf.concat([query_emb, listing_emb, title_emb], axis=1)
        elif self.nir_emb_fusion == "qtsl":
            mlp_input = tf.concat([query_emb, title_emb, shop_emb, listing_emb], axis=1)
        else:
            raise ValueError(f"Unsupported emb fusion {self.nir_emb_fusion}")

        logits = self.mlp(mlp_input)
        if output_logits:
            return logits
        else:
            return tf.nn.softmax(logits, axis=1)

    def get_input_emb(
        self, x, entity: Literal["query", "listing", "title", "shop"], training=False
    ):
        if entity == "query":
            embs = self.nir_wrapper.embed_query_features(x, training=training)
        elif entity == "listing":
            embs = self.nir_wrapper.embed_listing_features(x, training=training, use_hqi=False)
        elif entity == "title":
            embs = self.nir_wrapper.embed_title_features_as_query(x, training=training)
        elif entity == "shop":
            embs = self.nir_wrapper.embed_shop_features_as_query(x, training=training)
        else:
            raise ValueError(f"Unsupported entity {entity}")

        emb = embs[self.nir_emb_key]

        proj_layer = (self.nir_proj_layers or {}).get(entity)
        if proj_layer is not None:
            emb = proj_layer(emb)
        return emb

    def call(self, query_listing_features, training=False):
        listing_emb = self.get_input_emb(query_listing_features, "listing", training)
        listing_title_emb = self.get_input_emb(query_listing_features, "title", training)
        query_emb = self.get_input_emb(query_listing_features, "query", training)
        shop_emb = self.get_input_emb(query_listing_features, "shop", training)
        return self.score_query_listing(
            query_emb, listing_emb, listing_title_emb, shop_emb, self.call_fn_use_logits
        )

    def export_model(self, export_path):
        embedding_dim = self.get_embedding_dim()

        @tf.function(
            input_signature=[
                tf.TensorSpec(shape=[None, embedding_dim], dtype=tf.float32),
                tf.TensorSpec(shape=[None, embedding_dim], dtype=tf.float32),
                tf.TensorSpec(shape=[None, embedding_dim], dtype=tf.float32),
                tf.TensorSpec(shape=[None, embedding_dim], dtype=tf.float32),
            ]
        )
        def score_query_listing_proba(query_emb, listing_emb, title_emb, shop_emb):
            probas = self.score_query_listing(
                query_emb, listing_emb, title_emb, shop_emb, output_logits=False
            )
            return {"probas": probas}

        signatures = {
            "embed_listings": self.embed_listings.get_concrete_function(),
            "embed_queries": self.embed_queries.get_concrete_function(),
            "score_query_listing_proba": score_query_listing_proba.get_concrete_function(),
        }
        self.save(export_path, signatures=signatures, include_optimizer=False)


def v2_rep_dataframe_to_tf_dataset(
    dataframe: pd.DataFrame,
    label_key: str,
    nir_model: NirWrapper,
    batch_size=128,
    include_eval_columns=False,
) -> tf.data.Dataset:
    all_features = {QUERY_KEY} | set(V2_LISTING_STRING_COLUMNS)
    for f in list(all_features):
        # fill missing with empty string
        dataframe[f] = dataframe[f].fillna("")

    if LISTING_DESCRIPTION_KEYWORDS_KEY in dataframe.columns:
        dataframe[LISTING_DESCRIPTION_KEYWORDS_KEY] = dataframe[LISTING_DESCRIPTION_KEYWORDS_KEY].str.replace(";", ".")
        logger.info(dataframe[LISTING_DESCRIPTION_KEYWORDS_KEY].head(5))

    tensors = {
        f: tf.constant(dataframe[f], dtype=tf.string)
        for f in [
            QUERY_KEY,
            SHOP_NAME_KEY,
            LISTING_TITLE_KEY,
            LISTING_DESCRIPTION_KEYWORDS_KEY,
            LISTING_TAXO_KEY,
        ]
    }
    # in dataframe, we use 1-index relevance classes, here uses 0-index
    labels_3 = dataframe[LABEL_KEY].astype("int") - 1
    tensors["labels_3"] = tf.constant(labels_3)
    tensors["probas_3"] = tf.constant(list(dataframe[LABEL_PROBA_KEY]))
    tensors["source"] = tf.constant(dataframe["source"])
    features_to_copy = ["source", "labels_3", "probas_3"]

    if include_eval_columns:
        tensors[LISTING_ID_KEY] = tf.constant(dataframe[LISTING_ID_KEY].astype("int"))
        features_to_copy += [QUERY_KEY, LISTING_ID_KEY, SHOP_NAME_KEY]

    raw_dataset = tf.data.Dataset.from_tensor_slices(tensors).batch(batch_size)

    def preproc_batch(b):
        query_features = nir_model.preproc_query(b["query"])
        shop_features = nir_model.preproc_query(b["shopName"])
        shop_features = {k.replace("query", "shop"): v for k, v in shop_features.items()}
        listing_features = nir_model.preproc_listing(
            {"title": b["title"], "tags": b["descNgrams"], "taxonomyPath": b["taxonomyPath"]}
        )
        copied_features = {f: b[f] for f in features_to_copy}
        res = {**copied_features, **query_features, **listing_features, **shop_features}
        # res = {**copied_features, **shop_features}
        return (res, res[label_key])

    dataset = raw_dataset.map(preproc_batch, num_parallel_calls=tf.data.AUTOTUNE).prefetch(
        tf.data.AUTOTUNE
    )
    return dataset


@click.command()
@click.option("--config-path", type=str, required=True)
@click.option("--output-dir", type=str, required=True)
def main(config_path, output_dir):
    #  Load config
    # config_path = "kubeflow/configs/v2_serving_rep.yaml"
    # from semantic_relevance.training.v2_serving.rep_nirt import v2_rep_dataframe_to_tf_dataset, SemrelModelServingV2
    cfg = load_model_dev_config(config_path)

    #  Create output dir if not exists
    if not output_dir.startswith("gs://"):
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            os.makedirs(os.path.join(output_dir, "model"))
            logger.info(f"Created {output_dir}")
        else:
            logger.info(f"{output_dir} already exists")

    #  Load NIR-T as backbone
    nir_metadata = get_latest_nir_metadata(
        force_model_path="gs://etldata-prod-search-ranking-data-hkwv8r/data/shared/neural_ir/transformers_hqi_loose/models/2024-11-04/checkpoints/saved_model_04"
    )
    nir_model = NirWrapper.from_model_paths(nir_metadata.model_path, nir_metadata.tft_model_path)
    nir_model_date = nir_metadata.model_date
    logger.info(f"Done loading NIR-T as base. Metadata: {nir_metadata}")

    #  Establish dates
    end_date = cfg.data_end_date
    if end_date == "":
        # if end date is not specified in config, use the NIR model date
        end_date = nir_model_date
    eval_date = max(end_date, nir_model_date) + timedelta(days=1)
    logger.info(f"{end_date=}, {eval_date=}, {nir_model_date=}")
    logger.info(cfg)

    #  Build data for training and eval
    logger.info("Building training data")
    train_dfs = load_data_list(
        cfg.train_data,
        end_date=end_date,
        eval_date=eval_date,
        final_columns=V2_SERVING_DF_COLUMNS,
        mode="train",
    )
    logger.info("Building test data")
    test_dfs = load_data_list(
        cfg.eval_data,
        end_date=end_date,
        eval_date=eval_date,
        final_columns=V2_SERVING_DF_COLUMNS,
        mode="eval",
    )
    train_joint_df = combine_dfs(train_dfs)
    test_joint_df = combine_dfs(test_dfs)

    logger.info(f"Data columns = {list(train_joint_df.columns)}")
    logger.info(f"Train sample size = {train_joint_df.shape[0]}")
    logger.info(f"Test sample size = {test_joint_df.shape[0]}")
    logger.info(f"Train label distribution = \n {train_joint_df[LABEL_KEY].value_counts()}")
    logger.info(f"Test label distribution = \n {test_joint_df[LABEL_KEY].value_counts()}")
    logger.info("Done data loading")

    #  Convert to TF datasets
    logger.info("Convert pandas dataframe to TF datasets")
    if cfg.objective == "kl":
        label: TrainingLabel = "probas_3"
    elif cfg.objective == "crossent":
        label: TrainingLabel = "labels_3"
    else:
        raise ValueError(f"Unsupported model objective {cfg.objective}")

    train_dataset = v2_rep_dataframe_to_tf_dataset(
        train_joint_df, label_key=label, nir_model=nir_model, batch_size=cfg.batch_size
    )
    test_dataset = v2_rep_dataframe_to_tf_dataset(
        test_joint_df,
        label_key=label,
        nir_model=nir_model,
        batch_size=cfg.batch_size,
        include_eval_columns=True,
    )

    #  Train
    logger.info("Start training")
    optimizer = tf.keras.optimizers.Adam(learning_rate=cfg.learning_rate)

    if label == "labels_3":
        # Loss/metrics for classification
        loss = tf.keras.losses.SparseCategoricalCrossentropy(
            from_logits=True,
        )
        metrics = [
            tf.keras.metrics.SparseCategoricalAccuracy(name="accuracy"),
            AUCMultiClassToBinary(name="auc_1vrest", positive_ids=[1, 2], num_classes=3),
        ]
        output_logits = True
    elif label == "probas_3":
        loss = tf.keras.losses.KLDivergence()
        metrics = [tf.keras.metrics.KLDivergence()]
        output_logits = False
    else:
        raise ValueError(f"Unsupported label {label}")

    nir_model.trainable = not cfg.freeze_backbone
    semrel_model = SemrelModelServingV2(
        nir_wrapper=nir_model,
        n_classes=3,
        hidden_sizes=cfg.hidden_sizes,
        call_fn_use_logits=output_logits,
        nir_emb_key=cfg.embed_key,
        nir_emb_proj=cfg.embed_proj,
        nir_emb_fusion=cfg.embed_fusion,
    )

    semrel_model.compile(optimizer=optimizer, loss=loss, metrics=metrics)

    semrel_model.fit(
        x=train_dataset,
        validation_data=test_dataset,
        batch_size=cfg.batch_size,
        epochs=cfg.num_epochs,
        callbacks=[],
    )

    model_dir = os.path.join(output_dir, "model")
    semrel_model.export_model(model_dir)
    logger.info(f"Wrote model to: {model_dir}")


if __name__ == "__main__":
    main()
