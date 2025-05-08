from collections import defaultdict, Counter
from google.cloud import bigquery
import pandas as pd
import tensorflow as tf
import tensorflow_transform as tft
from tensorflow_transform.tf_metadata import schema_utils
import tensorflow_text
from typing import Dict, List, TypedDict, Optional
from utils import utils

nir_t_path = "gs://etldata-prod-search-ranking-data-hkwv8r/data/shared/neural_ir/transformers_hqi_loose/models/2024-03-09/checkpoints/saved_model_04"
nir_t = tf.saved_model.load(nir_t_path)


nir_t_spec = {
    "listingId": tf.io.FixedLenFeature([], tf.int64),
    **{
        k: tf.io.FixedLenFeature([], tf.string)
        for k in ["title", "tags", "taxonomyPath"]
    },
    **{
        k: tf.io.VarLenFeature(tf.string)
        for k in ["clickTopQuery", "cartTopQuery", "purchaseTopQuery"]
    },
}
nir_t_schema = schema_utils.schema_from_feature_spec(nir_t_spec)
nir_t_tfex_encoder = tft.coders.ExampleProtoCoder(nir_t_schema)


class NIRTExample(TypedDict):
    listingId: int
    title: str
    tags: str
    taxonomyPath: str


def create_listing_tf_examples(listings: List[NIRTExample]):
    # Returns 1-d string tensor with encoded TF examples
    return tf.constant([nir_t_tfex_encoder.encode(l) for l in listings])


def nir_t_embed_listings(
    listings: List[NIRTExample], hqi_from: Optional[List[str]] = None
):
    # hqi_from: If specified, empty out the HQI fields of a listing and set clickTopQuery to the listing fields in hqi_from
    if hqi_from is not None:
        blank_hqi = {"clickTopQuery": [], "cartTopQuery": [], "purchaseTopQuery": []}
        new_listings = []
        for l in listings:
            new_l = {**l, **blank_hqi}
            new_l["clickTopQuery"] = [l[f] for f in hqi_from]
            new_listings.append(new_l)
        listings = new_listings
    listing_tf_examples = create_listing_tf_examples(listings)
    return nir_t.signatures["embed_listings"](tf_examples=listing_tf_examples)[
        "embedding"
    ]


def nir_t_embed_queries(queries: List[str]):
    queries = tf.constant(queries)
    return nir_t.signatures["embed_raw_queries"](queries)["embedding"]


str_data = tf.constant(
    ["valentine home_decor", "christmas trees", "mother's day gifts", "t-shirts"],
    dtype=tf.string,
)
tmp = tf.constant(["query"])

a_embedding = nir_t.signatures["embed_raw_queries"](str_data)["embedding"]
b_embedding = nir_t.signatures["embed_raw_queries"](tmp)["embedding"]

cos_score = utils.dense_vector_cosine(a_embedding, b_embedding)
# queries = tf.constant([query])
query_embedding = nir_t.signatures["embed_raw_queries"](str_data)["embedding"]
query_embedding.shape

cos_score = utils.dense_vector_cosine(query_embedding[0, :], query_embedding[1, :])


nir_v2_path = "gs://etldata-prod-search-ranking-data-hkwv8r/data/shared/neural_ir/30d/large_voc_huge_hidden_assym/models/2024_03_13/training_dir/checkpoints/saved_model"
nir_v2 = tf.saved_model.load(nir_v2_path)
v2_embedding = nir_v2.signatures["embed_raw_queries"](str_data)["embedding"]
v2_embedding.shape
