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
