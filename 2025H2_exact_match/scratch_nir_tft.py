from typing import Optional
import tensorflow as tf
from semantic_relevance.utils.nir import get_latest_nir_metadata
from semantic_relevance.training.tensorflow_models.nirt_prod_model import (
    NirWrapper,
    NirModelWrapper,
    NirTFTLayer
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
    "clickTopQuery": tf.RaggedTensor.from_tensor(tf.zeros([3, 0], tf.string)).to_sparse(),
    "cartTopQuery": tf.RaggedTensor.from_tensor(tf.zeros([3, 0], tf.string)).to_sparse(), 
    "purchaseTopQuery": tf.RaggedTensor.from_tensor(tf.zeros([3, 0], tf.string)).to_sparse(),
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

type(query_results["query_char_3grams"])
tf.sparse.to_dense(query_results["query_char_3grams"]).shape
type(query_results["query_word_1grams"])
tf.sparse.to_dense(query_results["query_word_1grams"]).shape
listing_results["title"]
tf.sparse.to_dense(listing_results["taxonomyPath"]).shape
tf.sparse.to_dense(listing_results["tags_word_1grams"]).shape

# new TFT
new_tft_layer = NirTFTLayer()
new_query_results = new_tft_layer(query_input)
new_listing_results = new_tft_layer(listing_input)
sorted(new_query_results.keys())
sorted(new_listing_results.keys())

type(new_query_results["query_char_3grams"])
tf.sparse.to_dense(new_query_results["query_char_3grams"]).shape
type(new_query_results["query_word_1grams"])
tf.sparse.to_dense(new_query_results["query_word_1grams"]).shape
new_listing_results["title"]
tf.sparse.to_dense(new_listing_results["taxonomyPath"]).shape
tf.sparse.to_dense(new_listing_results["tags_word_1grams"]).shape
