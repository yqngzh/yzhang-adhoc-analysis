import json
from collections import Counter
from sklearn.metrics import f1_score, confusion_matrix, classification_report

GT_FIELD = "etsy_round_label"

feature_dict_options = {
    "tsidn": {
        "query": "query",
        "title": "en_title", 
        "shop": "listingShopName", 
        "image": "listingHeroImageCaption", 
        "description": "listingDescNgrams"
    },
    "all_listing": {
        "query": "query",
        "title": "en_title", 
        "shop": "listingShopName", 
        "image": "listingHeroImageCaption", 
        "description": "listingDescNgrams",
        "category": "listingTaxo",
        "attribute": "listingAttributes",
        "tag": "listingTags",
        "variation": "listingVariations",
        "review": "listingReviews"
    },
    "q_rewrites": {
        "query": "query",
        "query_rewrites": "queryRewrites",
        "title": "en_title", 
        "shop": "listingShopName", 
        "image": "listingHeroImageCaption", 
        "description": "listingDescNgrams"
    },
    "q_entities": {
        "query": "query",
        "query_entities": "queryEntities",
        "title": "en_title", 
        "shop": "listingShopName", 
        "image": "listingHeroImageCaption", 
        "description": "listingDescNgrams"
    },
    "full": {
        "query": "query",
        "query_rewrites": "queryRewrites",
        "query_entities": "queryEntities",
        "title": "en_title", 
        "shop": "listingShopName", 
        "image": "listingHeroImageCaption", 
        "description": "listingDescNgrams",
        "category": "listingTaxo",
        "attribute": "listingAttributes",
        "tag": "listingTags",
        "variation": "listingVariations",
        "review": "listingReviews"
    },
    "agg": {
        "query": "query",
        "query_entities": "queryEntities",
        "query_rewrites": "queryRewrites",
        "title": "en_title", 
        "shop": "listingShopName", 
        "image": "listingHeroImageCaption", 
        "description": "desc",
        "attribute": "listingAttributes",
        "variation": "listingVariations",
    }
}

query_feature_dict_options = {
    "query": {
        "query": "query",
    },
    "qe": {
        "query": "query",
        "query_entities": "queryEntities",
    },
    "qr": {
        "query": "query",
        "query_entities": "queryEntities",
    },
    "all": {
        "query": "query",
        "query_entities": "queryEntities",
        "query_rewrites": "queryRewrites",
    }
}

listing_feature_dict_options = {
   "tsidn": {
       "title": "en_title",
       "shop": "listingShopName", 
       "image": "listingHeroImageCaption", 
       "description": "listingDescNgrams"
    }
}


def build_json_data_v2(record, feature_dict):
    output = {k: "not available" for k in [
        "query", "query_rewrites", "query_entities", "title", "shop", "image",
        "description", "category", "attribute", "tag", "variation", "review"
    ]}
    for k, v in feature_dict.items():
        if record[v] != "":
            output[k] = record[v]
    return output


def build_json_data_v3(record, feature_dict):
    output = {k: "not available" for k in feature_dict.keys()}
    for k, v in feature_dict.items():
        if record[v] != "":
            output[k] = record[v]
    return output


def build_json_string(record, query_feature_dict, listing_feature_dict):
    product_dict = build_json_data_v3(record, listing_feature_dict)
    output = {
        "query": record["query"],
        "product": product_dict
    }
    return json.dumps(output, ensure_ascii=False)

    
def evaluate(y_gt, y_pred):
    f1 = f1_score(y_gt, y_pred, average="macro")
    print(f"{f1=}")
    cls_report = classification_report(y_gt, y_pred, digits=3)
    print(cls_report)
    conf_mat = confusion_matrix(y_gt, y_pred, labels=["relevant", "partial", "not_relevant"])
    print(conf_mat)

    
def compute_majority_label(label_vec):
    label_counter = Counter(label_vec)
    most_common_tuple = label_counter.most_common()[0]
    if most_common_tuple[1] == 1:
        found_majority = False
        majority_label = "partial"
    else:
        found_majority = True
        majority_label = most_common_tuple[0]
    
    return found_majority, majority_label
