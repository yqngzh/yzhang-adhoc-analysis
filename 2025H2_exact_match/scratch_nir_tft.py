import tensorflow as tf
from semantic_relevance.utils.nir import get_latest_nir_metadata
from semantic_relevance.training.tensorflow_models.nirt_prod_model import (
    NirWrapper,
    NirModelWrapper,
    _SUPPORTED_NIR_CONFIGS,
)


# data
query_tensor = tf.constant(['3d print fallout helmet', 'cheer', 'vegetable headdress'], tf.string)
title_tensor = tf.constant([
    'Tactical Skull Helmet Stand – 3D Printed Military Display Bust | Night Vision Skull Base | Helmet Mount Display for Gear, Cosplay, or Decor',
    '6 FONTS Glitter Cheer Bow Tag 30 COLORS available! Backpack Team Name Glitter AllStar Elite Personalized Customizable Cheerleader Gift Dance',
    'NUTRITION MONTH HEADDRESS Printable Healthy Food Paper Crown Template for Kids, Fruits Vegetable Crafts | Eating Healthy Fun Activities Pdf'
], tf.string)
tags_tensor = tf.constant([
    '.3D printed Skull.Helmet Display Stand.Tactical Helmet.military display.NVG skull stand.military stand.Milsim gear display.Skull bust 3d print.Tactical gear stand.night vision display.3D Skull prop.Gamer desk decor.combat helmet stand',
    '.Cheer.team.bag.tag.glitter.personalized.custom.name.bogg.sports.bow.keychain.dance',
    '.fruit and veggie.headdress crafts.printable headdress.printable crown.nutrition crown.kids paper crown.nutrition month.crown for kids.digital download.digital file.pdf file.digital products.headdress template'
], tf.string)
taxonomy_tensor = tf.constant([
    'accessories.costume_accessories.masks_and_prosthetics.masks',
    'accessories.keychains_and_lanyards.keychains',
    'paper_and_party_supplies.paper.stationery.design_and_templates.worksheets'
], tf.string) 

query_input = {"query": query_tensor}
listing_input = {
    "title": title_tensor,
    "tags": tags_tensor,
    "taxonomyPath": taxonomy_tensor,
}


# old TFT
metadata = get_latest_nir_metadata(
    nir_models_glob="gs://training-prod-search-data-jtzn/neural_ir/transformers-hqi-loose/models/*/checkpoints/saved_model_04", 
    is_loc_model=False
)
old_model = NirWrapper.from_model_paths(metadata.model_path, metadata.tft_model_path)
query_results = old_model.nir_tft(query_input)
listing_results = old_model.nir_tft(listing_input)
# query_results.keys()
# dict_keys(['query_char_3grams', 'query_word_1grams', 'query_word_2grams'])
# listing_results.keys()
# dict_keys([
#     'title', 
#     'title_char_3grams', 'title_word_1grams', 'title_word_2grams', 
#     'tags_char_3grams', 'tags_word_1grams', 'tags_word_2grams', 
#     'taxonomyPath'
# ])


# new TFT
_CHARACTER_REPLACEMENTS = {r"”": '"', r"’": "'"}
_DEFAULT_WHITESPACE_EQUIVALENT_CHARS = frozenset(
    [r"\p{Z}", r"\s", r"\\", r"\/", r"\-", r"\_", r"\|", "~", ";", ",", "\x00"]
)
NGRAM_FIELDS = {
    "query": {"word_separator": " ", "multivalue_separator_pattern": "\\."},
    "title": {"word_separator": " ", "multivalue_separator_pattern": "\\."},
    "tags": {"word_separator": " ", "multivalue_separator_pattern": "\\."},
}
NGRAM_CATEGORIES = {
    "char_3grams": {"type": "char", "n": 3, "top_k": 8000, "frequency_threshold": 50, "num_oov_buckets": 16000},
    "word_1grams": {"type": "word", "n": 1, "top_k": 200000, "frequency_threshold": 5, "num_oov_buckets": 400000},
    "word_2grams": {"type": "word", "n": 2, "top_k": 120000, "frequency_threshold": 5, "num_oov_buckets": 240000},
}
HIERARCHICAL_FIELD = {
    "taxonomyPath": {"separator": ".", "top_k": 2000, "frequency_threshold": 10, "num_oov_buckets": 2000}
}

input_dict_tensors = listing_input
whitespace_pattern = f"[{''.join(_DEFAULT_WHITESPACE_EQUIVALENT_CHARS)}]+"
character_replacements = _CHARACTER_REPLACEMENTS

def normalize_string_tensor(s):
    s = tf.strings.lower(s)
    s = tf.strings.regex_replace(s, whitespace_pattern, " ")
    for char, to_replace in character_replacements.items():
        s = tf.strings.regex_replace(s, char, to_replace)
    s = tf.strings.strip(s)
    return s

output = {}
for field_name, field_spec in NGRAM_FIELDS.items():
    if field_name in input_dict_tensors:
        raw_tensor = input_dict_tensors[field_name]
        cleaned_tensor = normalize_string_tensor(raw_tensor) 
        char_tokens: tf.RaggedTensor = tf.strings.unicode_split(
            field_spec["word_separator"] + cleaned_tensor + field_spec["word_separator"],
            input_encoding="UTF-8",
            errors="ignore",
        )
        word_tokens: tf.RaggedTensor = tf.strings.split(cleaned_tensor, field_spec["word_separator"])
        for ngram_cat_name, ngram_cat_spec in NGRAM_CATEGORIES.items():
            if ngram_cat_spec["type"] == "char":
                ngrams = tf.strings.ngrams(char_tokens, ngram_cat_spec["n"], separator="")
            elif ngram_cat_spec["type"] == "word":
                ngrams = tf.strings.ngrams(
                    word_tokens, ngram_cat_spec["n"], separator=field_spec["word_separator"]
                )
            ngrams = ngrams.to_sparse()
            output_field_name = field_name + "_" + ngram_cat_name
            output[output_field_name] = ngrams
