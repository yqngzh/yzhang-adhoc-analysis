# source: https://github.com/etsy/ComputerVisionResearch/blob/835014abfacfad38747d28a02fa4315707ea30c9/projects/complete_the_look/embedding_model/exploration/verify_embedding_v2.ipynb
import gcsfs
import pyarrow.parquet as pq
import json
import pandas as pd
import numpy as np
from google.cloud import storage
import tensorflow as tf
import functools as ft

BUCKET = "etsy-cv-ml-dev-scratch-data-hq40c1"
PREFIX = "ttl-60/shop_with_etsy_ctl_infer/test_inf_0727"


# Read parquet from GCS
def read_parquet(gs_directory_path, to_pandas=True):
    gs = gcsfs.GCSFileSystem()
    arrow_df = pq.ParquetDataset(gs_directory_path, filesystem=gs)
    if to_pandas:
        return arrow_df.read_pandas().to_pandas()
    return arrow_df


df_parquet = read_parquet(f"gs://{BUCKET}/{PREFIX}/parquet")


# Read tfrecords from GCS
raw_dataset = tf.data.TFRecordDataset(
    f"gs://{BUCKET}/{PREFIX}/tfrecords/tfrecords-00000-of-00001.tfrecord"
)
spec = {
    "listing_id": tf.io.FixedLenFeature([], tf.int64),
    "image_id": tf.io.FixedLenFeature([], tf.int64),
    "embedding": tf.io.FixedLenFeature([128], tf.float32),
    "class_id": tf.io.FixedLenFeature([], tf.int64),
}

df_list = []
for raw_record in raw_dataset:
    result = tf.io.parse_single_example(raw_record, spec)

    curr_res = {
        "listing_id": result["listing_id"].numpy(),
        "image_id": result["image_id"].numpy(),
        "embedding": result["embedding"].numpy(),
        "class_id": result["class_id"].numpy(),
    }
    df_list.append(curr_res)


df_tfrecord = pd.json_normalize(df_list)
