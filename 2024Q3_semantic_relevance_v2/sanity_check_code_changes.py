import click
from typing import Union, Dict
from tqdm import tqdm
from copy import deepcopy
from datetime import datetime
import pandas as pd
import numpy as np
from sklearn.metrics import f1_score, classification_report, auc, precision_recall_curve

import tensorflow as tf
import tensorflow_text  # noqa

from semantic_relevance.data.model_scores import get_hydrated_teacher_scores
from semantic_relevance.utils.data_loading import load_annotation_v2_data
from semantic_relevance.utils.constants import (
    TEACHER_V2,
    SHOP_NAME_KEY,
    LISTING_TITLE_KEY,
    LISTING_DESCRIPTION_KEY,
    LISTING_DESCRIPTION_KEYWORDS_KEY,
    LISTING_ATTRIBUTES_KEY,
    LISTING_ATTRIBUTES_CLEAN_KEY,
    LISTING_TAXO_KEY,
    LISTING_TAGS_KEY,
    QUERY_KEY,
    LISTING_CONCAT_TEXT_KEY,
)

V2_LISTING_FEATURES = {
    LISTING_TITLE_KEY: "title",
    SHOP_NAME_KEY: "shop",
    LISTING_ATTRIBUTES_CLEAN_KEY: "attribute",
    LISTING_DESCRIPTION_KEY: "description",
}

TSDN_FEATURES = {
    LISTING_TITLE_KEY: "title",
    SHOP_NAME_KEY: "shop",
    LISTING_DESCRIPTION_KEYWORDS_KEY: "description",
}


def get_listing_text_from_dataframe_row(
    row, listing_features: Union[str, Dict], lowercase: bool = False
):
    if isinstance(listing_features, str):
        listing_text = row[listing_features]
    elif isinstance(listing_features, dict):
        assert len(listing_features) > 0, "listing_features dictionary cannot be empty"
        listing_text = " ".join(
            f"<{feature_name}> {row[colname] if row[colname] is not None else ''}"
            for colname, feature_name in listing_features.items()
        )
    if lowercase:
        listing_text = listing_text.lower()
    return listing_text


def semrel_filtering_predict(model, queries, title, tags, taxo):
    query_embs = model.signatures["embed_queries"](queries)
    listing_embs = model.signatures["embed_listings"](
        title=title,
        tags=tags,
        taxonomyPath=taxo,
    )
    outputs = model.signatures["score_query_listing_proba"](
        query_emb=query_embs["query_emb"],
        listing_emb=listing_embs["listing_emb"],
        title_emb=listing_embs["title_emb"],
    )["probas"]
    return outputs


def rep_nirt_predict(model, queries, title, shops, desc_ngrams, taxo):
    query_embs = model.signatures["embed_queries"](queries)
    shop_embs = model.signatures["embed_queries"](shops)
    listing_embs = model.signatures["embed_listings"](
        title=title,
        tags=desc_ngrams,
        taxonomyPath=taxo,
    )
    outputs = model.signatures["score_query_listing_proba"](
        query_emb=query_embs["query_emb"],
        listing_emb=listing_embs["listing_emb"],
        title_emb=listing_embs["title_emb"],
        shop_emb=shop_embs["query_emb"],
    )["probas"]
    return outputs


def bert_ce_predict(model, queries, listings):
    outputs = model.signatures["serving_default"](
        queries=queries,
        listings=listings,
    )["softmax"]
    return outputs


def rep_encoder_predict(model, queries, listings):
    outputs = model.signatures["score_query_listing_proba"](
        queries=queries,
        listings=listings,
    )["probas"]
    return outputs


def predict(
    df,
    model,
    predict_fn,
    listing_fields,
    output_is_logits: bool = False,
    batch_size=64,
    debug=False,
    compute_shop_scores=False
):
    # fill missing
    columns_to_fill = [QUERY_KEY] + list(listing_fields.values())
    for f in columns_to_fill:
        df[f] = df[f].fillna("")

    n_batches = df.shape[0] // batch_size + 1
    preds = None
    shop_scores = None

    for i in tqdm(range(n_batches)):
        start_idx = i * batch_size
        end_idx = min((i + 1) * batch_size, df.shape[0])
        batch_df = df.iloc[start_idx:end_idx, :]

        queries = tf.constant(batch_df[QUERY_KEY], tf.string)
        listing_data = {k: tf.constant(batch_df[v], tf.string) for k, v in listing_fields.items()}

        batch_scores = predict_fn(model, queries=queries, **listing_data)

        if compute_shop_scores:
            query_embs = model.signatures["embed_queries"](queries)["query_emb"]
            shop_embs = model.signatures["embed_queries"](tf.constant(batch_df[SHOP_NAME_KEY]))["query_emb"]
            norm_query_embs = tf.math.l2_normalize(query_embs, axis=1)
            norm_shop_embs = tf.math.l2_normalize(shop_embs, axis=1)
            batch_cos_sim = tf.math.reduce_sum(tf.multiply(norm_query_embs, norm_shop_embs), axis=1, keepdims=True)

        if preds is None:
            preds = batch_scores
            if compute_shop_scores:
                shop_scores = batch_cos_sim
        else:
            preds = tf.concat([preds, batch_scores], axis=0)
            if compute_shop_scores:
                shop_scores = tf.concat([shop_scores, batch_cos_sim], axis=0)

    if output_is_logits:
        softmax_scores = tf.nn.softmax(preds)
    else:
        softmax_scores = preds
    pred_labels = tf.argmax(softmax_scores, axis=1) + 1  # 1-index labels

    softmax_np = softmax_scores.numpy()
    pred_labels_np = pred_labels.numpy()

    new_df = deepcopy(df)
    new_df.reset_index(inplace=True, drop=True)
    new_df["predicted_labels"] = pred_labels_np
    softmax_df = pd.DataFrame(
        data=softmax_np,
        index=list(range(softmax_np.shape[0])),
        columns=["score_not_relevant", "score_partial", "score_relevant"],
    )
    output_df = pd.concat([new_df, softmax_df], axis=1)

    if compute_shop_scores:
        shop_scores_np = shop_scores.numpy()
        output_df["shop_heu_scores"] = shop_scores_np

    return softmax_scores, pred_labels, output_df


@click.command()
@click.option("--saved-model-dir", type=str, required=True)
@click.option("--eval-date", type=str, required=True)
@click.option("--pred-function", type=str, required=True)
@click.option("--listing-feature-field", type=str, default="listing_texts")
# saved_model_dir = "gs://training-dev-search-data-jtzn/user/yzhang/semantic-relevance/rep_berts_v2_encoder_final_lh_1020_10m/model"; eval_date = "2024-10-21"; pred_function = "rep_encoder"
# from semantic_relevance.training.v2_serving.v2_serving_evaluate import predict, semrel_filtering_predict, bert_ce_predict, get_listing_text_from_dataframe_row
def main(saved_model_dir, eval_date, pred_function, listing_feature_field):
    # listing_fields: dict map from predict function arguments to column names in input df
    if pred_function == "semrel_filtering":
        listing_fields = {"title": listing_feature_field, "tags": "tags", "taxo": "taxonomyPath"}
        pred_fn = semrel_filtering_predict
        listing_text_dict = V2_LISTING_FEATURES
    elif pred_function == "bert_ce":
        listing_fields = {"listings": listing_feature_field}
        pred_fn = bert_ce_predict
        listing_text_dict = TSDN_FEATURES  # V2_LISTING_FEATURES
    elif pred_function == "rep_encoder":
        listing_fields = {"listings": listing_feature_field}
        pred_fn = rep_encoder_predict
        listing_text_dict = V2_LISTING_FEATURES  # TSDN_FEATURES
    elif pred_function == "rep_nirt":
        listing_fields = {
            "title": "title",
            "shops": "shopName",
            "desc_ngrams": "descNgrams",
            "taxo": "taxonomyPath",
        }
        pred_fn = rep_nirt_predict
        listing_text_dict = TSDN_FEATURES  # TSDN_FEATURES
    else:
        raise ValueError(f"Unsupported pred function {pred_function}")

    compute_shop_heuristic = False
    output_is_logits = False
    shop_df_path = "./datasets/shop_name_query_real_2024-10-22_dn.csv"

    if isinstance(eval_date, str):
        eval_date = datetime.strptime(eval_date, "%Y-%m-%d")

    #  LOAD MODEL
    model = tf.saved_model.load(saved_model_dir)

    #  EVALUATE ON HUMAN ANNOTATION V2 TEST
    #  Read V2 test split
    v2_test_df = load_annotation_v2_data("anno_v2_test")
    # concat listing texts
    v2_test_df[LISTING_CONCAT_TEXT_KEY] = v2_test_df.apply(
        get_listing_text_from_dataframe_row, axis=1, args=(listing_text_dict, False)
    )

    #  Evalaute on annotation V2 test split
    v2_softmax, v2_pred_labels, v2_res_df = predict(
        df=v2_test_df,
        model=model,
        predict_fn=pred_fn,
        listing_fields=listing_fields,
        output_is_logits=output_is_logits,
    )

    v2_macro_f1 = f1_score(v2_res_df.gold_label, v2_res_df.predicted_labels, average="macro")
    v2_cls_report = classification_report(
        v2_test_df.gold_label, v2_res_df.predicted_labels, digits=3
    )

    irrelevant_binary_label = v2_res_df.gold_label.map({1: 1, 2: 0, 3: 0})
    precision_irr, recall_irr, _ = precision_recall_curve(
        irrelevant_binary_label, v2_res_df.score_not_relevant
    )
    auprc_irrelevant = auc(recall_irr, precision_irr)

    relevant_binary_label = v2_res_df.gold_label.map({1: 0, 2: 0, 3: 1})
    precision_rel, recall_rel, _ = precision_recall_curve(
        relevant_binary_label, v2_res_df.score_relevant
    )
    auprc_relevant = auc(recall_rel, precision_rel)

    #  EVALUATE ON V2 TEACHER SCORES
    #  Read teacher label from eval_date
    teacher_eval_df = get_hydrated_teacher_scores(
        eval_date,
        eval_date,
        model_name=TEACHER_V2,
        extra_features=[
            SHOP_NAME_KEY,
            LISTING_DESCRIPTION_KEY,
            LISTING_DESCRIPTION_KEYWORDS_KEY,
            LISTING_ATTRIBUTES_KEY,
        ],
    )
    # clean attributes
    teacher_eval_df[LISTING_ATTRIBUTES_CLEAN_KEY] = (
        teacher_eval_df[LISTING_ATTRIBUTES_KEY].str.replace("#", ":").str.lower()
    )
    teacher_eval_df.drop(columns=[LISTING_ATTRIBUTES_KEY], inplace=True)
    # concat listing texts
    teacher_eval_df[LISTING_CONCAT_TEXT_KEY] = teacher_eval_df.apply(
        get_listing_text_from_dataframe_row, axis=1, args=(listing_text_dict, False)
    )

    #  Evaluate on teacher label from eval_date
    t_softmax, t_pred_labels, t_res_df = predict(
        df=teacher_eval_df,
        model=model,
        predict_fn=pred_fn,
        listing_fields=listing_fields,
        output_is_logits=output_is_logits,
        batch_size=128
    )
    pred_irr_score = t_res_df["score_not_relevant"]
    true_irr_score = t_res_df["gold_probas"].apply(lambda x: x[0])
    pred_rel_score = t_res_df["score_relevant"]
    true_rel_score = t_res_df["gold_probas"].apply(lambda x: x[2])

    corr_irr_pr = pred_irr_score.corr(true_irr_score)
    corr_irr_sp = pred_irr_score.corr(true_irr_score, method="spearman")
    mse_irr = np.square(np.subtract(true_irr_score.values, pred_irr_score.values)).mean()

    corr_rel_pr = pred_rel_score.corr(true_rel_score)
    corr_rel_sp = pred_rel_score.corr(true_rel_score, method="spearman")
    mse_rel = np.square(np.subtract(true_rel_score.values, pred_rel_score.values)).mean()

    #  EVALUATE ON SHOP QUERIES
    #  how shop query df is generated:
    # https://github.com/yqngzh/yzhang-adhoc-analysis/blob/master/2024Q3_semantic_relevance_v2/sample_real_shop_queries.ipynb
    shop_df = pd.read_csv(shop_df_path)
    shop_df.rename(
        columns={
            "listingTitle": LISTING_TITLE_KEY,
            "listingDescription": LISTING_DESCRIPTION_KEY,
            "listingTaxo": LISTING_TAXO_KEY,
            "listingTags": LISTING_TAGS_KEY,
            "shopName_x": SHOP_NAME_KEY,
        },
        inplace=True,
    )
    # clean attributes
    shop_df[LISTING_ATTRIBUTES_CLEAN_KEY] = (
        shop_df[LISTING_ATTRIBUTES_KEY].str.replace("#", ":").str.lower()
    )
    shop_df.drop(columns=[LISTING_ATTRIBUTES_KEY], inplace=True)
    # concat listing texts
    shop_df[LISTING_CONCAT_TEXT_KEY] = shop_df.apply(
        get_listing_text_from_dataframe_row, axis=1, args=(listing_text_dict, False)
    )

    shop_softmax, shop_pred_labels, shop_res_df = predict(
        df=shop_df,
        model=model,
        predict_fn=pred_fn,
        listing_fields=listing_fields,
        output_is_logits=output_is_logits,
        compute_shop_scores=compute_shop_heuristic,
    )

    label_wrong_pred = shop_pred_labels != 3
    
    if compute_shop_heuristic:
        shop_heu_misses = shop_res_df["shop_heu_scores"] < 0.5
        shop_errors = np.logical_and(label_wrong_pred, shop_heu_misses)
        shop_err_rate_with_heu = np.sum(shop_errors) / shop_df.shape[0]
        shop_err_rate_without_heu = np.sum(label_wrong_pred) / shop_df.shape[0]
    else:
        shop_err_rate = np.sum(label_wrong_pred) / shop_df.shape[0]

    #  PRINT RESULTS
    print(f"V2 macro F1: {np.round(v2_macro_f1, 3)}")
    print(f"AUPRC Irrelevant: {np.round(auprc_irrelevant, 3)}")
    print(f"AUPRC Relevant: {np.round(auprc_relevant, 3)}")
    print(f"Irrelevant - Pearson corr: {np.round(corr_irr_pr, 3)} ")
    print(f"Irrelevant - Spearman corr: {np.round(corr_irr_sp, 3)} ")
    print(f"Irrelevant - MSE: {np.round(mse_irr, 3)} ")
    print(f"Relevant - Pearson corr: {np.round(corr_rel_pr, 3)} ")
    print(f"Relevant - Spearman corr: {np.round(corr_rel_sp, 3)} ")
    print(f"Relevant - MSE: {np.round(mse_rel, 3)} ")
    if compute_shop_heuristic:
        print(f"Shop error rate NO heu: {np.round(shop_err_rate_without_heu, 3)}")
        print(f"Shop error rate With heu: {np.round(shop_err_rate_with_heu, 3)}")
    else:
        print(f"Shop error rate: {np.round(shop_err_rate, 3)}")
    print(v2_cls_report)


if __name__ == "__main__":
    main()