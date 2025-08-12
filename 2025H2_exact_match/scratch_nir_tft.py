from typing import Optional
import tensorflow as tf
from semantic_relevance.utils.nir import get_latest_nir_metadata
from semantic_relevance.training.tensorflow_models.nirt_prod_model import (
    NirWrapper,
    NirModelWrapper,
    _SUPPORTED_NIR_CONFIGS,
    _CHARACTER_REPLACEMENTS,
    _DEFAULT_WHITESPACE_EQUIVALENT_CHARS,
    NGRAM_FIELDS,
    NGRAM_CATEGORIES,
    HIERARCHICAL_FIELD,
    TensorLike
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
sorted(query_results.keys())
# dict_keys(['query_char_3grams', 'query_word_1grams', 'query_word_2grams'])
sorted(listing_results.keys())
# dict_keys([
#     'title', 
#     'title_char_3grams', 'title_word_1grams', 'title_word_2grams', 
#     'tags_char_3grams', 'tags_word_1grams', 'tags_word_2grams', 
#     'taxonomyPath'
# ])


# new TFT
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

def apply_vocabulary_with_lookup(
    token_tensor: tf.Tensor,
    vocab_filename_tensor: tf.Tensor,
    num_oov_buckets: int = 1,
    default_value: int | None = None,
    name: str | None = None,
):
    with tf.name_scope(name or "apply_vocab"):
        file_init = tf.lookup.TextFileInitializer(
            filename=vocab_filename_tensor,
            key_dtype=tf.string,
            key_index=tf.lookup.TextFileIndex.WHOLE_LINE,
            value_dtype=tf.int64,
            value_index=tf.lookup.TextFileIndex.LINE_NUMBER,
        )
        if num_oov_buckets > 0:
            if default_value is not None:
                # StaticVocabularyTable cannot also take a default_value – it uses OOV buckets.
                raise ValueError(
                    "When using StaticVocabularyTable (num_oov_buckets > 0), "
                    "`default_value` must be None."
                )
            table = tf.lookup.StaticVocabularyTable(
                initializer=file_init,
                num_oov_buckets=num_oov_buckets,
            )
            lookup_fn = table.lookup
        else:
            # No OOV buckets: use a StaticHashTable with a single default id.
            if default_value is None:
                default_value = -1  # mirror TFT’s common default when not provided
            table = tf.lookup.StaticHashTable(
                initializer=file_init,
                default_value=tf.cast(default_value, tf.int64),
            )
            lookup_fn = table.lookup
        # Support SparseTensor in/out to mirror tft.apply_vocabulary behavior.
        if isinstance(token_tensor, tf.SparseTensor):
            mapped_vals = lookup_fn(token_tensor.values)
            return tf.SparseTensor(
                indices=token_tensor.indices,
                values=mapped_vals,
                dense_shape=token_tensor.dense_shape,
            )
        else:
            return lookup_fn(token_tensor)

def apply_vocab_with_offset(
    token_tensor: tf.SparseTensor,
    vocab_filename_tensor: tf.Tensor,
    num_oov_buckets: int,
    default_value: int,
    offset: int = 0,
    name: Optional[str] = None,
) -> tf.SparseTensor:
    token_ids: tf.SparseTensor = apply_vocabulary_with_lookup(
        token_tensor,
        deferred_vocab_filename_tensor=vocab_filename_tensor,
        num_oov_buckets=num_oov_buckets,
        default_value=default_value,
        name=f"apply_vocab_{name}" if name else None,
    )
    if offset != 0:
        token_ids = tf.SparseTensor(
            indices=token_ids.indices,
            values=tf.add(
                token_ids.values, offset, name=f"offset_vocab_{offset}_{name or 'x'}"
            ),
            dense_shape=token_ids.dense_shape,
        )
    return token_ids

def rebuild_hierarchy_fn(a, x):
    new_val = tf.cond(
        tf.math.equal(a[0], x[0]),
        lambda: tf.stack([x[0], tf.strings.join([a[1], sep, x[1]])]),
        lambda: x,
    )
    return new_val
    
def expand_hierarchy_string(s: tf.Tensor, sep: str = " ") -> tf.RaggedTensor:
    parts = tf.strings.split(s, sep=sep)
    rowids = parts.value_rowids()
    rowid_and_part = tf.stack([tf.strings.as_string(rowids), parts.values], axis=1)
    init_vals = tf.stack([tf.constant("rowid"), tf.constant("part")])
    rowids_and_new_values = tf.scan(rebuild_hierarchy_fn, rowid_and_part, initializer=init_vals)
    new_values = rowids_and_new_values[:, 1]
    return tf.RaggedTensor.from_value_rowids(
        values=new_values, value_rowids=rowids, nrows=parts.nrows()
    )
        


output = {}
for field_name, field_spec in NGRAM_FIELDS.items():
    # field_name, field_spec = "title", NGRAM_FIELDS["title"]
    if field_name not in input_dict_tensors:
        continue

    raw_tensor = input_dict_tensors[field_name]
    cleaned_tensor = normalize_string_tensor(raw_tensor)

    field_sep = field_spec["word_separator"]
    char_tokens: tf.RaggedTensor = tf.strings.unicode_split(
        tf.strings.join([field_sep, cleaned_tensor, field_sep]), 
        input_encoding="UTF-8", errors="ignore"
    )
    word_tokens: tf.RaggedTensor = tf.strings.split(cleaned_tensor, field_sep)
    field_ngram_output = []
    for ngram_cat_name, ngram_cat_spec in NGRAM_CATEGORIES.items():
        # ngram_cat_name, ngram_cat_spec = "word_2grams", NGRAM_CATEGORIES["word_2grams"]
        if ngram_cat_spec["type"] == "char":
            ngrams = tf.strings.ngrams(char_tokens, ngram_cat_spec["n"], separator="")
        else:  # "word"
            ngrams = tf.strings.ngrams(word_tokens, ngram_cat_spec["n"], separator=field_sep)
        ngrams_sparse = ngrams.to_sparse()
        field_ngram_output.append((ngram_cat_name, ngrams_sparse))

