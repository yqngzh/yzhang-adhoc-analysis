import tensorflow as tf
import tensorflow_text as text
import tensorflow_ranking as tfr

saved_model_path = "gs://training-dev-search-data-jtzn/semantic_relevance/production_models/bert-cern-l24-h1024-a16-v1/export/saved_model"
# saved_model_path = "gs://training-dev-search-data-jtzn/semantic_relevance/production_models/bert-cern-l2-h128-a2-v1/export/saved_model"
# saved_model_path = "gs://etldata-prod-search-ranking-data-hkwv8r/users/ebenj/semrelv2_rehearsal/train2/model"

model = tf.saved_model.load(saved_model_path)

# query = tf.constant(
#     [
#         "summer clothing",
#         "summer clothing",
#         "summer clothing",
#         "summer clothing",
#         "will you marry me egg"
#     ],
#     dtype=tf.string,
# )
query = [
    "wedding dress",
]

# title = tf.constant(
#     [
#         "Bug Off Lotion Bar - Natural Insect Repellent, Solid Lotion Bar, Body Butter Bar, with Mango Butter, Beeswax, & Sunflower Oil",
#         "Personalized Handmade Women Gold Name Necklace, Minimalist Mothers day Gifts for Her, Personalized Gift for Women Who has everything",
#         "Surfer Girl Necklace, Initial Necklace, Waterproof Beach Jewelry, Women Jewels, Non-tarnish Accessory, Adjustable Length 36+5cm, Boho",
#         "Mothers Gift Box, Gift Box for Grandma, Birthday Gift Set, Self Care Gift for Mom, Daughter to Mother Gift, Hygge Gift Box, Basket for Her",
#         "Will You Marry Me Gift • Copper • Engagement Collection • Marry Me • Pressed Penny"
#     ],
#     dtype=tf.string,
# )
title = [
    "Bridal Hanger, Wedding Name Hanger, Personalized Hanger, Wedding Custom Hanger, Dress Hanger. Wedding Gifts"
]

outputs = model.signatures["serving_default"](
    queries=query,
    titles=title,
)
outputs["softmax"][:, 3]


# filtering V2 model
query_embs = model.signatures['embed_queries'](query)
listing_embs = model.signatures['embed_listings'](
    title=title,
    tags=title,
    taxonomyPath=title,
)
probas = model.signatures['score_query_listing_proba'](
    query_emb=query_embs["query_emb"], listing_emb=listing_embs["listing_emb"], title_emb=listing_embs["title_emb"]
)["probas"]


gains_05 = tf.expand_dims(tf.constant([0.0, 0.5, 0.5, 1.0]), 1)
pred_expected_gain = tf.transpose(tf.matmul(outputs["softmax"], gains_05))
pred_softmax_3 = tf.expand_dims(outputs["softmax"][:, 3], 0)
pred_logits_3 = tf.expand_dims(outputs["logits"][:, 3], 0)

test_gain = tf.constant([0.0, 0.5, 0.5, 1.0], dtype=tf.float32)
test_expected_gain = tf.reduce_sum(outputs["softmax"] * test_gain, axis=-1)

expected_gain_ranks = tfr.utils.sorted_ranks(pred_expected_gain)
expected_gain_ranks = tf.squeeze(tf.cast(expected_gain_ranks, tf.float32))

softmax_ranks = tfr.utils.sorted_ranks(pred_softmax_3)
softmax_ranks = tf.squeeze(tf.cast(softmax_ranks, tf.float32))

logits_ranks = tfr.utils.sorted_ranks(pred_logits_3)
logits_ranks = tf.squeeze(tf.cast(logits_ranks, tf.float32))


sem_rel_labels = tf.argmax(outputs["softmax"], axis=1)
sem_rel_labels = tf.constant([3, 1, 2, 1, 0], dtype=tf.int64)
rel_graded_targets = tf.where(
    sem_rel_labels == 0, 0.0, tf.where(sem_rel_labels == 3, 1.0, 0.5)
)


sem_rel_labels = tf.constant([3, 1, 2, 1, 0], dtype=tf.int64)
rel_graded_targets = tf.where(sem_rel_labels == 0, 0.0, 0.01)
rel_graded_targets = tf.where(sem_rel_labels == 3, 1.0, rel_graded_targets)
rel_graded_targets = tf.where(sem_rel_labels == 2, 0.1, rel_graded_targets)

alpha = 0.1
purchase_scores = tf.constant(
    [0.1, 0.9, 3.2, 1.0, 3.5], dtype=tf.float32
)  # 5, 4, 2, 3, 1
relevance_scores = tf.constant(
    [0.998, 0.997, 0.681, 0.999, 0.9983], dtype=tf.float32
)  # 3, 4, 5, 1, 2

batch_relevance_scores = tf.expand_dims(relevance_scores, axis=0)
relevance_ranks = tfr.utils.sorted_ranks(batch_relevance_scores)
relevance_ranks = tf.cast(relevance_ranks, tf.float32)
relevance_ranks = tf.squeeze(relevance_ranks)

batch_purchase_scores = tf.expand_dims(purchase_scores, axis=0)
purchase_ranks = tfr.utils.sorted_ranks(batch_purchase_scores)
purchase_ranks = tf.cast(purchase_ranks, tf.float32)
purchase_ranks = tf.squeeze(purchase_ranks)

agg_ranks = (1.0 - alpha) * purchase_ranks + alpha * relevance_ranks
agg_scores = 1.0 / agg_ranks
agg_scores
# [0.20833333, 0.25      , 0.43478262, 0.3571429 , 0.9090909 ]
# 5, 4, 2, 3, 1
agg_ranks = tfr.utils.sorted_ranks(tf.expand_dims(agg_scores, 0))
purchase_ranks = tfr.utils.sorted_ranks(tf.expand_dims(purchase_scores, 0))


scores = -1.0 * tf.constant([list(range(46))], dtype=tf.float32)
y_true = tf.constant(
    [
        [
            0.6487165093421940,
            0.99592834711074800,
            0.72321522235870400,
            0.99234265089035000,
            0.99474656581878700,
            0.99700778722763100,
            0.9946058988571170,
            0.99723291397094700,
            0.996906042098999,
            0.99764752388000500,
            0.98955577611923200,
            0.99437373876571700,
            0.98865240812301600,
            0.74430131912231400,
            0.9954841136932370,
            0.98742395639419600,
            0.9990496039390560,
            0.60909408330917400,
            0.99710625410080000,
            0.99683672189712500,
            0.98609417676925700,
            0.99592864513397200,
            0.99609977006912200,
            0.99447280168533300,
            0.98565870523452800,
            0.99529308080673200,
            0.99282032251358000,
            0.56766575574874900,
            0.99627029895782500,
            0.99649912118911700,
            0.99613755941391000,
            0.99565404653549200,
            0.98205870389938400,
            0.99817878007888800,
            0.99618613719940200,
            0.972722053527832,
            0.99285167455673200,
            0.59243500232696500,
            0.60943931341171300,
            0.64813554286956800,
            0.56988018751144400,
            0.997343897819519,
            0.59593093395233200,
            0.59949082136154200,
            0.583492636680603,
            0.973650336265564,
        ]
    ],
    dtype=tf.float32,
)
y_true
score_ranks = tfr.utils.sorted_ranks(scores)
true_ranks = tfr.utils.sorted_ranks(y_true)

# ndcg = tfr.keras.metrics.NDCGMetric(topn=5)
ndcg = tfr.keras.metrics.NDCGMetric()
ndcg._metric.compute(y_true, scores)

ndcg._metric.compute(
    -1.0 * tf.cast(true_ranks, tf.float32), -1.0 * tf.cast(scores, tf.float32)
)


from scipy.stats import spearmanr, pearsonr


def get_spearman_cor(y_true, y_pred):
    return tf.py_function(
        spearmanr,
        [tf.cast(y_pred, tf.float32), tf.cast(y_true, tf.float32)],
        Tout=tf.float32,
    )


def get_pearson_cor(y_true, y_pred):
    return tf.py_function(
        pearsonr,
        [tf.cast(y_pred, tf.float32), tf.cast(y_true, tf.float32)],
        Tout=tf.float32,
    )


get_pearson_cor(sem_rel_labels, purchase_scores)


####################
import tensorflow as tf
import tensorflow_text 
from google.cloud import bigquery

# QUERY = ["heirloom gift", "harry potter", "motor", "taurus gift"]
# LISTING_ID = [1237647786, 937073078, 1188722821, 1703022292]
QUERY = ["tower of pizza earrings"]
LISTING_ID = [1103572642]

model_path = "gs://training-dev-search-data-jtzn/semantic_relevance/production_models/v2-deberta-v3-large-tad/export/saved_model/"
v2_teacher = tf.saved_model.load(model_path)

query_str = f"""select 
    key AS listingId,
    IFNULL(
        COALESCE(NULLIF(verticaListings_title, ""), verticaListingTranslations_primaryLanguageTitle),
        ""
    ) title,
    IFNULL(verticaListings_description, "") description,
    IFNULL(verticaListings_tags, "") tags,
    IFNULL(verticaListings_taxonomyPath, "") taxonomyPath,
    (SELECT STRING_AGG(element, ';') FROM UNNEST(kbAttributesV2_sellerAttributesV2.list)) AS listingAttributes,
    IFNULL(verticaSellerBasics_shopName, "") shopName
from `etsy-ml-systems-prod.feature_bank_v2.listing_feature_bank_most_recent`
where key in ({','.join([str(x) for x in LISTING_ID])})
"""

client = bigquery.Client(project="etsy-search-ml-dev")
query_job = client.query(query_str)
rows = query_job.result()
df = rows.to_dataframe()

df['listingAttributesClean'] = df['listingAttributes'].str.replace("#",':').str.lower()

def build_listing_text(row):
    """take a row from dataframe, return listing text value"""
    listing_text_dict = {
        "title": row["title"],
        "shop": row["shopName"],
        "attribute": row["listingAttributesClean"],
        "description": row["description"],
    }
    listing_text = " ".join(
        f"<{k}> {v if v is not None else ''}" for k, v in listing_text_dict.items()
    )
    return listing_text

df["listing_text"] = df.apply(build_listing_text, axis=1)

query_texts = tf.constant([QUERY], tf.string)
listing_texts = tf.constant([df["listing_text"].values], tf.string)

outputs = v2_teacher.signatures["serving_default"](
    query_texts=query_texts, 
    listing_texts=listing_texts,
)
outputs["softmax"]


import pandas as pd
import numpy as np
from tqdm import tqdm

shop_df = pd.read_csv("/home/yzhang/development/0_yzhang_local/shop_name_query_real_2024-10-22_dn.csv")
shop_df.rename(
    columns={
        "listingTitle": "title",
        "listingDescription": "description",
        "shopName_x": "shopName",
    },
    inplace=True,
)

columns_to_fill = ["query", "title", "description", "listingAttributes", "shopName"]
for f in columns_to_fill:
    shop_df[f] = shop_df[f].fillna("")

shop_df["listingAttributesClean"] = shop_df["listingAttributes"].str.replace("#", ":").str.lower()
shop_df["listing_text"] = shop_df.apply(build_listing_text, axis=1)

batch_size = 32
n_batches = shop_df.shape[0] // batch_size + 1
preds = None

for i in tqdm(range(n_batches)):
    start_idx = i * batch_size
    end_idx = min((i + 1) * batch_size, shop_df.shape[0])
    batch_df = shop_df.iloc[start_idx:end_idx, :]
    queries = tf.constant(batch_df["query"], tf.string)
    listing_data = tf.constant(batch_df["listing_text"], tf.string)
    batch_scores = v2_teacher.signatures["serving_default"](
        query_texts=queries, 
        listing_texts=listing_data,
    )["softmax"]
    if preds is None:
        preds = batch_scores
    else:
        preds = tf.concat([preds, batch_scores], axis=0)


pred_labels = tf.argmax(preds, axis=1) + 1  # 1-index labels
np.sum(pred_labels != 3) / shop_df.shape[0]

##############

import tensorflow as tf
import tensorflow_text as text

semantic_relevance_model = tf.saved_model.load("gs://etldata-prod-search-ranking-data-hkwv8r/data/shared/semrel/semrel_tf_serving_model/2024-09-01")


query = tf.constant(["summer clothing"] * 4, dtype=tf.string)

title = tf.constant(
    [
        "Bug Off Lotion Bar - Natural Insect Repellent, Solid Lotion Bar, Body Butter Bar, with Mango Butter, Beeswax, & Sunflower Oil",
        "Personalized Handmade Women Gold Name Necklace, Minimalist Mothers day Gifts for Her, Personalized Gift for Women Who has everything",
        "Surfer Girl Necklace, Initial Necklace, Waterproof Beach Jewelry, Women Jewels, Non-tarnish Accessory, Adjustable Length 36+5cm, Boho",
        "Mothers Gift Box, Gift Box for Grandma, Birthday Gift Set, Self Care Gift for Mom, Daughter to Mother Gift, Hygge Gift Box, Basket for Her",
    ],
    dtype=tf.string,
)

tags = tf.constant(
    [
        "Bug Off Lotion Bar - Natural Insect Repellent, Solid Lotion Bar, Body Butter Bar, with Mango Butter, Beeswax, & Sunflower Oil",
        "Personalized Handmade Women Gold Name Necklace, Minimalist Mothers day Gifts for Her, Personalized Gift for Women Who has everything",
        "Surfer Girl Necklace, Initial Necklace, Waterproof Beach Jewelry, Women Jewels, Non-tarnish Accessory, Adjustable Length 36+5cm, Boho",
        "Mothers Gift Box, Gift Box for Grandma, Birthday Gift Set, Self Care Gift for Mom, Daughter to Mother Gift, Hygge Gift Box, Basket for Her",
    ],
    dtype=tf.string,
)

taxonomy_paths = tf.constant(
    [
        "Bug Off Lotion Bar - Natural Insect Repellent, Solid Lotion Bar, Body Butter Bar, with Mango Butter, Beeswax, & Sunflower Oil",
        "Personalized Handmade Women Gold Name Necklace, Minimalist Mothers day Gifts for Her, Personalized Gift for Women Who has everything",
        "Surfer Girl Necklace, Initial Necklace, Waterproof Beach Jewelry, Women Jewels, Non-tarnish Accessory, Adjustable Length 36+5cm, Boho",
        "Mothers Gift Box, Gift Box for Grandma, Birthday Gift Set, Self Care Gift for Mom, Daughter to Mother Gift, Hygge Gift Box, Basket for Her",
    ],
    dtype=tf.string,
)

query_embs = semantic_relevance_model.signatures["embed_queries"](query)
listing_embs = semantic_relevance_model.signatures["embed_listings"](
    title=title,
    tags=tags,
    taxonomyPath=taxonomy_paths,
)

outputs = semantic_relevance_model.signatures["score_query_listing_proba"](
    query_emb=query_embs["query_emb"],
    listing_emb=listing_embs["listing_emb"],
    title_emb=listing_embs["title_emb"],
)["probas"]

##################

import tensorflow as tf
import tensorflow_text as text
import tensorflow_ranking as tfr
from typing import List

saved_model_path = "gs://training-dev-search-data-jtzn/semantic_relevance/production_models/bert-cern-l2-h128-a2-v1/export/saved_model"
model = tf.saved_model.load(saved_model_path)

query = tf.constant(
    ["summer clothing"] * 5,
    dtype=tf.string,
)
title = tf.constant(
    [
        "Bug Off Lotion Bar - Natural Insect Repellent, Solid Lotion Bar, Body Butter Bar, with Mango Butter, Beeswax, & Sunflower Oil",
        "Personalized Handmade Women Gold Name Necklace, Minimalist Mothers day Gifts for Her, Personalized Gift for Women Who has everything",
        "Surfer Girl Necklace, Initial Necklace, Waterproof Beach Jewelry, Women Jewels, Non-tarnish Accessory, Adjustable Length 36+5cm, Boho",
        "Mothers Gift Box, Gift Box for Grandma, Birthday Gift Set, Self Care Gift for Mom, Daughter to Mother Gift, Hygge Gift Box, Basket for Her",
        "Wholesale Lot Of Indian Vintage Kantha Quilt Handmade Throw Reversible Blanket Bedspread Cotton Fabric BOHEMIAN quilting Twin Size Bed cover"
    ],
    dtype=tf.string,
)
outputs = model.signatures["serving_default"](
    queries=query,
    titles=title,
)
softmax_scores = outputs["softmax"]
relevance_label = tf.argmax(softmax_scores, axis=1)
# relevant item index: 2, 3, 5
relevance_label = tf.constant(
    [0, 3, 0, 0, 0], dtype=tf.float32
)
tf.reduce_sum(tf.where(relevance_label == 3, 1, 0)).numpy()

purchase_scores = tf.constant(
    [0.1, 0.9, 1.0, 3.5, 3.2], dtype=tf.float32
)  # order of the items: 4, 5, 3, 2, 1


def normalize_scores(score_list: List[float]) -> List[float]:
    if len(score_list) == 0:
        return score_list
    else:
        min_score = min(score_list)
        max_score = max(score_list)
        if min_score == max_score:
            return [1.0] * len(score_list)
        else:
            return [
                (score - min_score) / (max_score - min_score)
                for score in score_list
            ]


normalized_purchase_scores = normalize_scores(
    purchase_scores.numpy().tolist()
)
normalized_purchase_scores = tf.constant(
    normalized_purchase_scores, dtype=tf.float32
)


######### only take 2 totally relevant items 
######### target order: 5, 3, 4, 2, 1
######### relevance label [0, 3, 0, 0, 0]
######### target order: 2, 4, 5, 3, 1
# count relevant in list
count_total_relevant = tf.reduce_sum(
    tf.where(relevance_label == 3, 1, 0)
).numpy()

# use alpha to control number of relevant items to promote
N = 2
N = min(N, count_total_relevant)

# purchase score for only relevant items
relevant_purchase_score = tf.where(
    relevance_label == 3,
    normalized_purchase_scores,
    0.0,
)
# rank relevant items by descending purchase score
relevant_purchase_rank = (
    tf.argsort(relevant_purchase_score, direction="DESCENDING", stable=True)
    .numpy()
    .tolist()
)
relevant_to_promote_index = relevant_purchase_rank[:N]


relevant_to_promote = [0] * len(relevant_purchase_rank)
for idx in relevant_to_promote_index:
    relevant_to_promote[idx] = 1
relevant_to_promote = tf.constant(relevant_to_promote, dtype=tf.int32)


agg_scores = tf.where(
    relevant_to_promote == 1,
    normalized_purchase_scores + 1.0,
    normalized_purchase_scores,
)
tf.argsort(
    agg_scores, direction='DESCENDING', stable=True, name=None
) + 1



######### relevant items first
######### target order: 5, 3, 2, 4, 1
agg_scores = tf.where(
    relevance_label == 3,
    normalized_purchase_scores + 1.0,
    normalized_purchase_scores,
)
tf.argsort(
    agg_scores, direction='DESCENDING', stable=True, name=None
) + 1
