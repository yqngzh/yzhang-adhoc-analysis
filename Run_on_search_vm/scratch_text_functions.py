import tensorflow as tf
from utils import utils

x_keys = tf.ragged.constant(
    [
        ["valentines day", "valentine day"],
        ["hat", "hat", "tails"],
        ["pipe", "pipe"],
    ],
    dtype=tf.string,
).to_sparse()

x_values = tf.ragged.constant(
    [
        [0.1, 0.2],
        [0.3, 0.4, 0.5],
        [0.6, 0.7],
    ],
    dtype=tf.float32,
).to_sparse()

y_keys = tf.ragged.constant(
    [
        ["valentines day", "a", "b"],
        ["hat", "tails"],
        ["pipe"],
    ],
    dtype=tf.string,
).to_sparse()

y_values = tf.ragged.constant(
    [
        [2.0, 1.0, 1.0],
        [4.0, 6.0],
        [8.0],
    ],
    dtype=tf.float32,
).to_sparse()

utils.dictionary_jaccard_similarity(x_keys, y_keys, distance_metric="overlap_size")
utils.dictionary_cosine_similarity(x_keys, x_values, y_keys, y_values)

tokens = tf.sets.intersection(x_keys, y_keys)
mask = tf.cast(tf.sparse.to_dense(tokens) != b"", tf.float32)
tf.reduce_sum(mask, axis=1)
tf.sets.size(x_keys)
tf.sets.size(y_keys)


sparse_tensor = tf.ragged.constant(
    [
        ["valentines day", "valentine day"],
        ["hat", "hat", "tails"],
        ["pipe", "pipe"],
    ],
    dtype=tf.string,
).to_sparse()

unique_sparse = tf.sets.intersection(sparse_tensor, sparse_tensor)
dense = tf.sparse.to_dense(unique_sparse)
size = tf.math.count_nonzero(
    dense,
    axis=1,
    dtype=tf.float32,
)
size
