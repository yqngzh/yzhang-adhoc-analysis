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

#############################
import tensorflow as tf

data = tf.constant(
    ["david allen planner", "bumble bee knitting pattern", "mother's day gifts", "t-shirts", "silver confetti overlay"],
    dtype=tf.string,
)
data = tf.strings.lower(data)

tf.strings.regex_full_match(data, ".*plan.*")

keywords = [
    "plan", "stl", "overlay", "pattern", "template", "design", "mockup", "checklist", 
    "meme", "emote", "file", "digital", "dst", "reading", "tarot", "poster", "qr",
    "print", "font", "logo", "editable", "excel"
]
query_digital_keyword_match = tf.strings.regex_full_match(data, ".*" + keywords[0] + ".*")
for i in range(1, len(keywords)):
    query_digital_keyword_match = tf.math.logical_or(query_digital_keyword_match, tf.strings.regex_full_match(data, ".*" + keywords[i] + ".*"))

query_digital_keyword_match
###################################

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from typing import Dict, List, Tuple, Optional, Any, Iterable

import tensorflow_transform as tft
from utils.preprocess.config import load_preprocess_config
from utils import beam_fn, utils

transform_fn_dir = "gs://training-dev-search-data-jtzn/neural_ranking/second_pass/yz-qtxt-web-allfeature-stopwd-7d/model_root/preprocess_fit/2024-02-01"

end_date = "2024-02-01"
data_modes = ["sampled"]
num_of_days = [1]
input_base_path_list = [
    "gs://etldata-prod-search-ranking-data-hkwv8r/feature_logging_training_data_parquet/query_pipeline_web_organic/loose_purchase"
]
dated_input_file_list = utils.get_dated_input_file_list(
    end_date=end_date,
    num_of_days=num_of_days,
    input_base_path_list=input_base_path_list,
    mode=data_modes,
    convert_loose=True,
)
# print(f"{dated_input_file_list=}")

# train_valid_split = [90, 10]
# train_valid_split = [round(100 * i / sum(train_valid_split)) for i in train_valid_split]
# num_partitions = len(train_valid_split)

# # Load transform function created in preprocess_fit
tft_output = tft.TFTransformOutput(transform_fn_dir)

# # For preparing raw data
raw_feature_spec = tft_output.raw_feature_spec()
config = load_preprocess_config(
    "/home/yzhang/development/neural_ranking/second_pass/pipeline/configs/preprocess/sr_web.yaml|query_text_process"
)
raw_defaults = config.get_raw_defaults()

# transformed_feature_spec = tft_output.transformed_feature_spec()
# example_feature_spec, context_feature_spec = get_ec_specs(transformed_feature_spec)

_CHARACTER_REPLACEMENTS = {r"”": '"', r"’": "'"}
_DEFAULT_WHITESPACE_EQUIVALENT_CHARS = [
    r"\s+",
    r"\\",
    r"\/",
    r"\-",
    r"\_",
    r"\|",
    "[^\w\s]",
    "\x00",
    "%20",
]

# source: https://github.etsycorp.com/Engineering/BigData/blob/master/operators/src/main/resources/nlp/stopwords/en.txt
# source2: https://github.com/etsy/pyspark_ml/blob/f9368fa7644d126acee12fa256b867d1eb5c3c45/modules/ps_listing_desc/ps_listing_desc/__init__.py#L34-L49
# removed i, we, you, they, their (e.g. gift for her)
STOPWORDS = [
    "&",
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "but",
    "by",
    "for",
    "if",
    "in",
    "into",
    "is",
    "it",
    "no",
    "not",
    "of",
    "on",
    "or",
    "such",
    "that",
    "the",
    "then",
    "there",
    "these",
    "this",
    "to",
    "was",
    "will",
    "with",
    "please",
    "s",
    "t",
    "let",
    "u",
    "n",
    "ha",
    "mm",
    "cm",
    "x",
]

text = "Embroidery_  _Fonts"

import re
from functools import partial
import nltk
from nltk.corpus import wordnet
from nltk.stem import WordNetLemmatizer
import krovetzstemmer


def light_clean(text: str) -> str:
    """Light cleaning for text"""
    text = text.lower()
    for char, replacement in _CHARACTER_REPLACEMENTS.items():
        text = text.replace(char, replacement)
    for char in _DEFAULT_WHITESPACE_EQUIVALENT_CHARS:
        pattern = re.compile(char)
        text = re.sub(pattern, " ", text)
    while "  " in text:
        text = text.replace("  ", " ")
    text = text.lstrip()
    text = text.rstrip()
    return text


def deep_clean(
    text: str, rm_stopword=False, stem_func=WordNetLemmatizer().lemmatize
) -> str:
    """K stemming with option for stopword removal"""
    text = light_clean(text)
    tokens = text.split(" ")
    stemmed_tokens = [stem_func(token) for token in tokens]
    if rm_stopword:
        stemmed_tokens = [token for token in stemmed_tokens if token not in STOPWORDS]
    text = " ".join(stemmed_tokens)
    return text


class TextProcessing(beam.DoFn):
    def __init__(self, feature_name_list: List[str], process_method_list: List[str]):
        if len(feature_name_list) != len(process_method_list):
            raise ValueError(
                "feature_name_list and process_method_list must have the same length"
            )
        self._feature_name_list = feature_name_list
        self._process_method_list = process_method_list

    def process(self, feature_dict: Dict):
        """Returns flattened_listings with text features processed"""
        for feature_name, process_method in zip(
            self._feature_name_list, self._process_method_list
        ):
            if process_method == "light":
                processing_func = light_clean
            elif process_method == "kstem":
                processing_func = partial(
                    deep_clean,
                    rm_stopword=False,
                    stem_func=krovetzstemmer.Stemmer().stem,
                )
            elif process_method == "wordnetlemma":
                processing_func = partial(
                    deep_clean,
                    rm_stopword=False,
                    stem_func=WordNetLemmatizer().lemmatize,
                )
            elif process_method == "kstem_stopword":
                processing_func = partial(
                    deep_clean,
                    rm_stopword=True,
                    stem_func=krovetzstemmer.Stemmer().stem,
                )
            elif process_method == "wordnetlemma_stopword":
                processing_func = partial(
                    deep_clean,
                    rm_stopword=True,
                    stem_func=WordNetLemmatizer().lemmatize,
                )
            else:
                raise ValueError(
                    f"process_method {process_method} is not supported. Supported methods are ['light', 'kstem', ''kstem_stopword', 'wordnetlemma', 'wordnetlemma_stopword']"
                )

            # request, list of listing, each listing has a list of strings
            input_feature = feature_dict[feature_name]

            input_feature_cleaned = []
            for listing_feature in input_feature:
                listing_feature_cleaned = []
                # clean each string for this listing
                for text in listing_feature:
                    listing_feature_cleaned.append(processing_func(text))

                # de-duplicate & keep order for this listing
                listing_feature_cleaned = [
                    element
                    for idx, element in enumerate(listing_feature_cleaned)
                    if element not in listing_feature_cleaned[:idx]
                ]
                input_feature_cleaned.append(listing_feature_cleaned)
                print(f"{listing_feature=}")
                print(f"{listing_feature_cleaned=}")

            feature_dict[feature_name] = input_feature_cleaned

        return [feature_dict]


with beam.Pipeline() as pipeline:
    grouped_flattened_listings = (
        pipeline
        | "CreateFileList" >> beam.Create(dated_input_file_list)
        | "ReadFiles"
        >> beam.io.ReadAllFromParquet(
            with_filename=True,
            columns=list(raw_feature_spec.keys()),
        )
    )

    grouped_flattened_listings = (
        grouped_flattened_listings
        | "FilterGroupData"
        >> beam.ParDo(
            beam_fn.FilterGroupData(
                min_request_length=28,
                us_only=True,
                remove_missing_context=True,
            )
        )
    )

    (
        grouped_flattened_listings
        | "ReplaceMissingFeatures"
        >> beam.ParDo(beam_fn.ReplaceDefaults(raw_defaults=raw_defaults))
        | "RemoveReturns"
        >> beam.ParDo(
            beam_fn.RemoveReturns(
                remove_returns=True,
                top_n_returned_listings=500,
            )
        )
        | "FilterIrrelevantListingPurchases"
        >> beam.ParDo(
            beam_fn.FilterIrrelevantListingPurchases(
                remove_irrelevant_purchases=True,
            )
        )
        | "TextProcessing"
        >> beam.ParDo(
            TextProcessing(
                feature_name_list=config.engineered.query_text_process_features,
                process_method_list=["wordnetlemma_stopword"]
                * len(config.engineered.query_text_process_features),
            )
        )
    )


############
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
