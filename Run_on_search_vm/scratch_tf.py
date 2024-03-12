import tensorflow as tf
import tensorflow_transform as tft
import nltk
from nltk.stem.snowball import SnowballStemmer
from nltk.corpus import wordnet
from nltk.stem import WordNetLemmatizer
import krovetzstemmer

str_lst_data = tf.ragged.constant(
    [
        ["trinity%20ring", "trio  trinity ring", "trinity rolling rings"],
        ["brass   stencil with word boston"],
        ["punchbowl;pipes", "punchbowl pipe"],
    ],
    dtype=tf.string,
).to_sparse()  # string list - sparse

str_data = tf.constant(
    ["valentine home_decor", "christmas trees", "mother's day gifts", "t-shirts"],
    dtype=tf.string,
)  # string - dense


_CHARACTER_REPLACEMENTS = {r"”": '"', r"’": "'"}
_DEFAULT_WHITESPACE_EQUIVALENT_CHARS = frozenset(
    [r"\p{Z}", r"\s", r"\\", r"\/", r"\-", r"\_", r"\|", "~", ";", ",", "\x00", "%20"]
)
STOPWORDS = ["trees", "pipe", "ring"]


def light_clean(feature):
    # To lower case
    feature = tf.strings.lower(feature)
    # Collapse unicode whitespace and selected symbols to single whitespace
    if len(_DEFAULT_WHITESPACE_EQUIVALENT_CHARS) > 0:
        replace_with_whitespace_ptn = (
            f"[{''.join(_DEFAULT_WHITESPACE_EQUIVALENT_CHARS)}]+"
        )
        feature = tf.strings.regex_replace(feature, replace_with_whitespace_ptn, " ")
    # Perform character replacements
    for char, to_replace in _CHARACTER_REPLACEMENTS.items():
        feature = tf.strings.regex_replace(feature, char, to_replace)
    # Trim leading and trailing whitespace
    feature = tf.strings.strip(feature)
    return feature


def sparse_light_clean(feature):
    """take sparse feature"""
    feature = tf.sparse.to_dense(feature)
    feature = light_clean(feature)
    feature = tf.RaggedTensor.from_tensor(feature, padding="").to_sparse()
    return feature


def clean_rmstopwords(feature):
    feature = light_clean(feature)
    feature_words = tf.strings.split(feature, " ")
    stopword_removed = tf.strings.regex_replace(
        feature_words, r"\b(" + r"|".join(STOPWORDS) + r")\b\s*", ""
    )
    output = tf.strings.reduce_join(stopword_removed, axis=-1, separator=" ")
    output = tf.strings.strip(output)
    return output


def sparse_clean_rmstopwords(feature):
    feature = tf.sparse.to_dense(feature)
    feature = clean_rmstopwords(feature)
    feature = feature.to_tensor()
    feature = tf.RaggedTensor.from_tensor(feature, padding="").to_sparse()
    return feature


stemmed_str_data = clean_rmstopwords(str_data)
print(f"{str_data=}")
print(f"{stemmed_str_data=}")

stemmed_str_lst_data = sparse_clean_rmstopwords(str_lst_data)
print(f"{str_lst_data=}")
print(f"{stemmed_str_lst_data=}")


##################  DEPRECATED  ##################
# def sparse_dict_light_clean(key_feature, value_feature):
#     key_feature = tf.sparse.to_dense(key_feature)
#     value_feature = tf.sparse.to_dense(value_feature)
#     key_feature = light_clean(key_feature)
#     value_feature = value_feature * tf.cast(key_feature != "", tf.float32)
#     key_feature = tf.RaggedTensor.from_tensor(key_feature, padding="").to_sparse()
#     value_feature = tf.RaggedTensor.from_tensor(value_feature, padding=0).to_sparse()
#     return key_feature, value_feature


def str_stemming(str_data, stemmer, rm_stopwords):
    tokens = str_data.decode().split(" ")
    stemmed_tokens = [stemmer.stem(token) for token in tokens]
    if rm_stopwords:
        stemmed_tokens = [token for token in stemmed_tokens if token not in STOPWORDS]
    concat_str = " ".join(stemmed_tokens)
    stemmed_str = stemmer.stem(concat_str)
    return stemmed_str


def str_lst_stemming(str_lst, stemmer, rm_stopwords):
    output = []
    for phrase in str_lst:
        stemmed_phrase = str_stemming(phrase, stemmer, rm_stopwords)
        output.append(stemmed_phrase)
    output = tf.constant(output)
    return output


def text_process_feature(feature, stem_algo="", rm_stopwords=False):
    cleaned_feature = light_clean(feature)
    stemmer = ""
    if stem_algo != "":
        if stem_algo == "kstem":
            stemmer = krovetzstemmer.Stemmer()
        elif stem_algo == "snowball":
            stemmer = SnowballStemmer("english")
        elif stem_algo == "wnetlemma":
            stemmer = WordNetLemmatizer()
        cleaned_feature_npy = cleaned_feature.numpy()
        if not isinstance(cleaned_feature_npy, bytes):
            output_feature = str_lst_stemming(
                cleaned_feature_npy, stemmer, rm_stopwords
            )
        else:
            output_feature = str_stemming(cleaned_feature_npy, stemmer, rm_stopwords)
    else:
        output_feature = cleaned_feature
    return output_feature


from functools import partial
import tensorflow_transform.beam as tft_beam

# tft_beam.Context(force_tf_compat_v1=True)


# def tf_text_process_light(feature):
#     return tft.apply_pyfunc(
#         text_process_feature,
#         tf.string,
#         False,
#         "text_process_light",
#         feature,
#         "",
#         False,
#     )


def tf_text_process_kstem(feature):
    return tft.apply_pyfunc(
        text_process_feature,
        [feature, "kstem", False],
        tf.string,
    )


def tf_text_process_kstem_rmstopwords(feature):
    return tft.apply_pyfunc(
        text_process_feature,
        [feature, "kstem", True],
        tf.string,
    )


def tf_text_process_wnet(feature):
    return tft.apply_pyfunc(
        text_process_feature,
        [feature, "wnetlemma", False],
        tf.string,
    )


def tf_text_process_wnet_rmstopwords(feature):
    return tft.apply_pyfunc(
        text_process_feature,
        [feature, "wnetlemma", True],
        tf.string,
    )


def tf_text_process_sparse_wrapper(sparse_feature, process_func):
    # convert to dense
    dense_feature = tf.sparse.to_dense(sparse_feature)
    # process
    processed_feature = tf.map_fn(process_func, dense_feature)
    # remove added empty strings
    # processed_feature = tf.ragged.boolean_mask(
    #     processed_feature, processed_feature != ""
    # )
    # remove duplicates
    unique_values = tf.map_fn(lambda row: tf.unique(row)[0], processed_feature)
    # back to sparse
    # output = unique_values.to_sparse()
    output = tf.sparse.from_dense(unique_values)
    return output


# stemmed_str_lst_data = tf_text_process_sparse_wrapper(
#     str_lst_data, tf_text_process_kstem_rmstopwords
# )

# def sparse_dict_light_clean(key_feature, value_feature):
#     key_feature = tf.sparse.to_dense(key_feature)
#     value_feature = tf.sparse.to_dense(value_feature)
#     key_feature = light_clean(key_feature)
#     value_feature = value_feature * tf.cast(key_feature == "", tf.float32)
#     key_feature = tf.RaggedTensor.from_tensor(key_feature, padding="").to_sparse()
#     value_feature = tf.RaggedTensor.from_tensor(value_feature, padding=0).to_sparse()
#     return key_feature, value_feature


# key_feature = tf.ragged.constant(
#     [
#         ["_"],
#         ["concept.do_it_yourself"],
#         ["hats", "hats_"],
#     ],
#     dtype=tf.string,
# ).to_sparse()

# value_feature = tf.ragged.constant(
#     [
#         [1.0],
#         [2.0],
#         [3.0, 4.0],
#     ],
#     dtype=tf.float32,
# ).to_sparse()

# new_key, new_value = sparse_dict_light_clean(key_feature, value_feature)
# print(f"{new_key=}")
# print(f"{new_value=}")
# tmp = tf.sparse.to_dense(new_key)
# print(f"{tmp=}")
