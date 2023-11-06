import math
from typing import Dict, Tuple, Union, List

import numpy
import tensorflow as tf
import tensorflow_transform as tft

UNDEFINED_FLOAT = float("nan")
DEFAULT_MONTHLY_SCORES_LIST = [0.0] * 12

# Keeping all comment lines for reference
# Feature groups and default values defined here
BOOL_FEATURES = {
    "candidateInfo.docInfo.listingInfo.activeListingBasics.newListingFlag": 0,
    "candidateInfo.docInfo.listingInfo.activeListingShippingCosts.hasProcessingTime": 0,
    "candidateInfo.docInfo.listingInfo.activeListingShippingCosts.shipsToUs": 0,
    # "candidateInfo.docInfo.shopInfo.verticaSellerBasics.aboutPage": 0,
    # "candidateInfo.docInfo.shopInfo.verticaSellerBasics.hasLabelsRev": 0,
    # "contextualInfo[name=target].docInfo.queryInfo.queryLevelMetrics.isDigital": 0,
    "candidateInfo.docInfo.listingInfo.activeListingShippingCosts.shipsFreeDomestic": 0,
    "candidateInfo.docInfo.listingInfo.activeListingShippingCosts.shipsFreeToUs": 0,
}

INT_FEATURES = {
    "candidateInfo.docInfo.listingInfo.activeListingBasics.daysSinceCreate": 0,
    "candidateInfo.docInfo.listingInfo.activeListingBasics.daysSinceOriginalCreate": 0,
    # "candidateInfo.docInfo.listingInfo.activeListingBasics.gmsDecile": 0,
    "candidateInfo.docInfo.listingInfo.activeListingBasics.gmsPercentile": 0,
    # "candidateInfo.docInfo.listingInfo.activeListingBasics.gmsQuintile": 0,
    # "candidateInfo.docInfo.listingInfo.activeListingBasics.gmsVigintile": 0,
    "candidateInfo.docInfo.listingInfo.activeListingBasics.imageCount": 0,
    "candidateInfo.docInfo.listingInfo.activeListingBasics.materialCount": 0,
    "candidateInfo.docInfo.listingInfo.activeListingBasics.pastDayOrders": 0,
    "candidateInfo.docInfo.listingInfo.activeListingBasics.pastDayQuantitySold": 0,
    # "candidateInfo.docInfo.listingInfo.activeListingBasics.pastYearOrders": 0,
    # "candidateInfo.docInfo.listingInfo.activeListingBasics.pastYearQuantitySold": 0,
    "candidateInfo.docInfo.listingInfo.activeListingBasics.quantity": 0,
    "candidateInfo.docInfo.listingInfo.activeListingBasics.totalFavoriteCount": 0,
    "candidateInfo.docInfo.listingInfo.activeListingBasics.totalOrders": 0,
    "candidateInfo.docInfo.listingInfo.activeListingBasics.totalQuantitySold": 0,
    "candidateInfo.docInfo.listingInfo.activeListingBasics.totalViewCount": 0,
    "candidateInfo.docInfo.listingInfo.activeListingShippingCosts.maxProcessingDays": 0,
    "candidateInfo.docInfo.listingInfo.activeListingShippingCosts.minProcessingDays": 0,
    "candidateInfo.docInfo.listingInfo.dwellTime.pastWeekTotalDwellMs": 0,
    # "candidateInfo.docInfo.listingInfo.recsysBasic.clicksCount21d": 0,
    # "candidateInfo.docInfo.listingInfo.recsysBasic.purchasesCount21d": 0,
    # "candidateInfo.docInfo.listingInfo.verticaListings.price": 0,  # in local currency cents
    "candidateInfo.docInfo.shopInfo.shopAvgRatings.count": 0,
    # "candidateInfo.docInfo.shopInfo.verticaSellerBasics.activeListings": 0,
    # "candidateInfo.docInfo.shopInfo.verticaSellerBasics.daysToHighPotentialSeller": 0,
    # "candidateInfo.docInfo.shopInfo.verticaSellerBasics.daysToPowerSeller": 0,
    # "candidateInfo.docInfo.shopInfo.verticaSellerBasics.daysToTopSeller": 0,
    # "candidateInfo.docInfo.shopInfo.verticaSellerBasics.forumPosts": 0,
    # "candidateInfo.docInfo.shopInfo.verticaSellerBasics.listingViews": 0,
    # "candidateInfo.docInfo.shopInfo.verticaSellerBasics.openToSaleDays": 0,
    "candidateInfo.docInfo.shopInfo.verticaSellerBasics.pastYearQuantitySold": 0,
    "candidateInfo.docInfo.shopInfo.verticaSellerBasics.shopImpressions": 0,
    "candidateInfo.docInfo.shopInfo.verticaSellerBasics.shopViews": 0,
    # "candidateInfo.docInfo.shopInfo.verticaSellerBasics.signedInVisitsPastThirty": 0,
    "candidateInfo.docInfo.shopInfo.verticaSellerBasics.totalOrders": 0,
    "candidateInfo.docInfo.shopInfo.verticaSellerBasics.totalQuantitySold": 0,
    "contextualInfo[name=target].docInfo.queryInfo.queryLevelMetrics.totalPurchases": 0,
    # "contextualInfo[name=user].userInfo.visitStat12m.visitDays": 0,
    # "contextualInfo[name=user].userInfo.visitStats.visitDays": 0,
}

FLOAT_FEATURES = {
    "candidateInfo.docInfo.listingInfo.activeListingBasics.maxPriceUsd": UNDEFINED_FLOAT,  # in dollors
    "candidateInfo.docInfo.listingInfo.activeListingBasics.normalizedDailyGms": UNDEFINED_FLOAT,
    "candidateInfo.docInfo.listingInfo.activeListingBasics.normalizedDailyQuantity": UNDEFINED_FLOAT,
    "candidateInfo.docInfo.listingInfo.activeListingBasics.pastDayGms": UNDEFINED_FLOAT,
    # "candidateInfo.docInfo.listingInfo.activeListingBasics.pastYearGms": UNDEFINED_FLOAT,
    "candidateInfo.docInfo.listingInfo.activeListingBasics.priceUsd": UNDEFINED_FLOAT,  # in dollors
    "candidateInfo.docInfo.listingInfo.activeListingBasics.totalGms": UNDEFINED_FLOAT,  # in dollors
    "candidateInfo.docInfo.listingInfo.activeListingShippingCosts.domesticShipCost": UNDEFINED_FLOAT,  # in dollors
    "candidateInfo.docInfo.listingInfo.activeListingShippingCosts.usShipCost": UNDEFINED_FLOAT,  # in dollors
    # "candidateInfo.docInfo.listingInfo.bfpPriceOptimization.avgSecondCatPrice": UNDEFINED_FLOAT,  # in dollors
    # "candidateInfo.docInfo.listingInfo.bfpPriceOptimization.avgTopCatPrice": UNDEFINED_FLOAT,  # in dollors
    # "candidateInfo.docInfo.listingInfo.kbGiftScores.overallGiftinessScore": UNDEFINED_FLOAT,
    # "candidateInfo.docInfo.listingInfo.listingProlistVisit30d.numCarts": UNDEFINED_FLOAT,
    # "candidateInfo.docInfo.listingInfo.listingProlistVisit30d.numImpressions": UNDEFINED_FLOAT,
    # "candidateInfo.docInfo.listingInfo.listingProlistVisit30d.smoothCart": UNDEFINED_FLOAT,
    # "candidateInfo.docInfo.listingInfo.listingProlistVisit30d.smoothCtr": UNDEFINED_FLOAT,
    "candidateInfo.docInfo.listingInfo.listingSearchVisit14d.avgPosition": UNDEFINED_FLOAT,
    "candidateInfo.docInfo.listingInfo.listingSearchVisit14d.avgPropensity": UNDEFINED_FLOAT,
    "candidateInfo.docInfo.listingInfo.listingSearchVisit14d.numCarts": UNDEFINED_FLOAT,
    "candidateInfo.docInfo.listingInfo.listingSearchVisit14d.numClicks": UNDEFINED_FLOAT,
    "candidateInfo.docInfo.listingInfo.listingSearchVisit14d.numImpressions": UNDEFINED_FLOAT,
    "candidateInfo.docInfo.listingInfo.listingSearchVisit14d.smoothCart": UNDEFINED_FLOAT,
    "candidateInfo.docInfo.listingInfo.listingSearchVisit14d.smoothCtr": UNDEFINED_FLOAT,
    "candidateInfo.docInfo.listingInfo.listingSmoothHistorical.smoothCartAdd": UNDEFINED_FLOAT,
    # "candidateInfo.docInfo.listingInfo.listingSmoothHistorical.smoothCtr": UNDEFINED_FLOAT,
    "candidateInfo.docInfo.listingInfo.listingSmoothHistorical.smoothPurchase": UNDEFINED_FLOAT,
    "candidateInfo.docInfo.shopInfo.shopAvgRatings.avgRating": UNDEFINED_FLOAT,
    "candidateInfo.docInfo.shopInfo.shopProlistVisit30d.smoothCart": UNDEFINED_FLOAT,
    "candidateInfo.docInfo.shopInfo.shopSmoothHistorical.smoothCartAdd": UNDEFINED_FLOAT,
    "candidateInfo.docInfo.shopInfo.shopSmoothHistorical.smoothCtr": UNDEFINED_FLOAT,
    "candidateInfo.docInfo.shopInfo.shopSmoothHistorical.smoothPurchase": UNDEFINED_FLOAT,
    # "candidateInfo.docInfo.shopInfo.verticaSellerBasics.avgActiveListingPriceUsd": UNDEFINED_FLOAT,
    # "candidateInfo.docInfo.shopInfo.verticaSellerBasics.pastYearGms": UNDEFINED_FLOAT,
    # "candidateInfo.docInfo.shopInfo.verticaSellerBasics.totalDomesticGms": UNDEFINED_FLOAT,
    # "candidateInfo.docInfo.shopInfo.verticaSellerBasics.totalGms": UNDEFINED_FLOAT,
    # "candidateInfo.docInfo.shopInfo.verticaSellerBasics.totalIntlGms": UNDEFINED_FLOAT,
    # "clientProvidedInfo.user.locationLatitude": UNDEFINED_FLOAT,
    # "clientProvidedInfo.user.locationLongitude": UNDEFINED_FLOAT,
    # "contextualInfo[name=target].docInfo.queryInfo.queryKBGiftFeatures.queryOverallGiftinessScore": UNDEFINED_FLOAT,
    # "contextualInfo[name=target].docInfo.queryInfo.queryLevelMetrics.cartRate": UNDEFINED_FLOAT,
    # "contextualInfo[name=target].docInfo.queryInfo.queryLevelMetrics.clickRate": UNDEFINED_FLOAT,
    # "contextualInfo[name=target].docInfo.queryInfo.queryLevelMetrics.purchaseEntropy": UNDEFINED_FLOAT,
    # "contextualInfo[name=target].docInfo.queryInfo.queryLevelMetrics.purchaseRate": UNDEFINED_FLOAT,
    "contextualInfo[name=target].docInfo.queryInfo.trebuchetQuery.avgClickPrice": UNDEFINED_FLOAT,  # in cents
    # "contextualInfo[name=target].docInfo.queryInfo.trebuchetQuery.avgDwellTime": UNDEFINED_FLOAT,
    "contextualInfo[name=target].docInfo.queryInfo.trebuchetQuery.avgPurchasePrice": UNDEFINED_FLOAT,  # in cents
    # "contextualInfo[name=target].docInfo.queryInfo.trebuchetQuery.logImpressionCount": UNDEFINED_FLOAT,
    # "contextualInfo[name=target].docInfo.queryInfo.trebuchetQuery.logNoListingsDwelled": UNDEFINED_FLOAT,
    # "contextualInfo[name=target].docInfo.queryInfo.trebuchetQuery.logTotalCarts": UNDEFINED_FLOAT,
    # "contextualInfo[name=target].docInfo.queryInfo.trebuchetQuery.logTotalCartsInVisit": UNDEFINED_FLOAT,
    # "contextualInfo[name=target].docInfo.queryInfo.trebuchetQuery.logTotalClicks": UNDEFINED_FLOAT,
    # "contextualInfo[name=target].docInfo.queryInfo.trebuchetQuery.logTotalClicksInVisit": UNDEFINED_FLOAT,
    # "contextualInfo[name=target].docInfo.queryInfo.trebuchetQuery.logTotalGmsInVisit": UNDEFINED_FLOAT,
    # "contextualInfo[name=target].docInfo.queryInfo.trebuchetQuery.logTotalPurchases": UNDEFINED_FLOAT,
    # "contextualInfo[name=target].docInfo.queryInfo.trebuchetQuery.logTotalPurchasesInVisit": UNDEFINED_FLOAT,
    # "contextualInfo[name=target].docInfo.queryInfo.trebuchetQuery.logTotalRevenueInVisit": UNDEFINED_FLOAT,
    # "contextualInfo[name=target].docInfo.queryInfo.trebuchetQuery.stdPurchasePrice": UNDEFINED_FLOAT,  # in cents
    "contextualInfo[name=user].userInfo.activeUserShipToLocations.latitude": UNDEFINED_FLOAT,
    "contextualInfo[name=user].userInfo.activeUserShipToLocations.longitude": UNDEFINED_FLOAT,
    "contextualInfo[name=user].userInfo.purchaseLtd.avgOrderValue": UNDEFINED_FLOAT,
    # "contextualInfo[name=user].userInfo.userKBGiftFeatures.buyerGiftCart21d": UNDEFINED_FLOAT,
    # "contextualInfo[name=user].userInfo.userKBGiftFeatures.buyerGiftClick21d": UNDEFINED_FLOAT,
    # "contextualInfo[name=user].userInfo.userKBGiftFeatures.buyerGiftPurchase1000d": UNDEFINED_FLOAT,
    "candidateInfo.docInfo.listingInfo.imageQualityScore.imageQualityScore": UNDEFINED_FLOAT,
}

TEXT_FEATURES = {
    # "candidateInfo.docInfo.listingInfo.activeListingBasics.countryName": "",
    # "candidateInfo.docInfo.listingInfo.activeListingBasics.mcEngagement": "",
    # "candidateInfo.docInfo.listingInfo.activeListingBasics.mcIntent": "",
    # "candidateInfo.docInfo.listingInfo.activeListingBasics.mcSuccess": "",
    "candidateInfo.docInfo.listingInfo.activeListingLocation.latitude": "",
    "candidateInfo.docInfo.listingInfo.activeListingLocation.longitude": "",
    "candidateInfo.docInfo.listingInfo.activeListingLocation.originPostalCode": "",
    # "candidateInfo.docInfo.listingInfo.verticaListings.currencyCode": "",
    # "candidateInfo.docInfo.listingInfo.verticaListings.path": "",
    "candidateInfo.docInfo.listingInfo.verticaListings.tags": "",
    "candidateInfo.docInfo.listingInfo.verticaListings.taxonomyPath": "",
    "candidateInfo.docInfo.listingInfo.verticaListings.title": "",
    "candidateInfo.docInfo.shopInfo.verticaSellerBasics.shopName": "",
    # "candidateInfo.docInfo.shopInfo.verticaSellerBasics.topCategoryNew": "",
    "clientProvidedInfo.user.locationZip": "",
    "contextualInfo[name=target].docInfo.queryInfo.query": "",
    # "contextualInfo[name=target].docInfo.queryInfo.queryBuyerTaxoDresden.taxoPath": "",
    # "contextualInfo[name=target].docInfo.queryInfo.queryLevelMetrics.bin": "",
    # "contextualInfo[name=user].userInfo.activeUserShipToLocations.state": "",
    "contextualInfo[name=user].userInfo.activeUserShipToLocations.zip": "",
    # "contextualInfo[name=user].userInfo.coarseGrained.recentTagsList": "",
    # "contextualInfo[name=user].userInfo.coarseGrained.topTagsList": "",
    "requestUUID": "",
}

ID_FEATURES = {
    "candidateInfo.docInfo.listingInfo.activeListingBasics.taxonomyId": -1,
    "candidateInfo.docInfo.listingInfo.activeListingLocation.originCountryId": -1,
    # "candidateInfo.docInfo.listingInfo.activeListingShippingCosts.shippingProfileId": -1,
    # "candidateInfo.docInfo.listingInfo.activeListingUSCarrier.carrierId": -1,
    "candidateInfo.docInfo.listingInfo.listingId": -1,
    "candidateInfo.docInfo.listingInfo.verticaListings.shopId": -1,
    # "contextualInfo[name=target].docInfo.queryInfo.queryBuyerTaxoDresden.taxoId": -1,
    "contextualInfo[name=user].userInfo.activeUserShipToLocations.countryId": -1,
    "contextualInfo[name=user].userInfo.userId": -1,
}

STRING_LIST_FEATURES = {
    # "candidateInfo.docInfo.listingInfo.kbNiches.niches": [],  # lenght 7
    # "candidateInfo.docInfo.listingInfo.topQuery.cartTopQuery": [],  # lenght 0-5
    # "candidateInfo.docInfo.listingInfo.topQuery.clickTopQuery": [],  # lenght 0-5
    # "candidateInfo.docInfo.listingInfo.topQuery.purchaseTopQuery": [],  # lenght 0-5
    # "candidateInfo.docInfo.listingInfo.topQueryYear.cartTopQuery": [],  # lenght 0-10
    "candidateInfo.docInfo.listingInfo.topQueryYear.clickTopQuery": [],  # lenght 0-10
    # "candidateInfo.docInfo.listingInfo.topQueryYear.purchaseTopQuery": [],  # lenght 0-10
    # add queryTaxoDemand features for future use
    # "contextualInfo[name=target].docInfo.queryInfo.queryTaxoDemandFeatures.clickTopTaxonomyPaths": [str], #length 0-50
    # "contextualInfo[name=target].docInfo.queryInfo.queryTaxoDemandFeatures.clickLevel2TaxonomyPaths": [str], #length 0-50
    # "contextualInfo[name=target].docInfo.queryInfo.queryTaxoDemandFeatures.purchaseTopTaxonomyPaths": [str], #length 0-50
    # "contextualInfo[name=target].docInfo.queryInfo.queryTaxoDemandFeatures.purchaseLevel2TaxonomyPaths": [str], #length 0-50
}
# more query taxo demand features
INT_LIST_FEATURES = {
    # "contextualInfo[name=target].docInfo.queryInfo.queryTaxoDemandFeatures.clickTopTaxonomyCounts": [int], #length 0-50
    # "contextualInfo[name=target].docInfo.queryInfo.queryTaxoDemandFeatures.clickLevel2TaxonomyCounts": [int], #length 0-50
    # "contextualInfo[name=target].docInfo.queryInfo.queryTaxoDemandFeatures.purchaseTopTaxonomyCounts": [int], #length 0-50
    # "contextualInfo[name=target].docInfo.queryInfo.queryTaxoDemandFeatures.purchaseLevel2TaxonomyCounts": [int], #length 0-50
}

VPCG_KEYS_FEATURES = {
    "candidateInfo.docInfo.listingInfo.coarseGrainedVpcg.listingIdCartsVPCG#keys": []
}

VPCG_VALUES_FEATURES = {}

DENSE_VECTOR_FEATURES = {
    # "candidateInfo.docInfo.listingInfo.kbGiftScores.monthlyGiftinessScores": DEFAULT_MONTHLY_SCORES_LIST,
    # "contextualInfo[name=target].docInfo.queryInfo.queryKBGiftFeatures.queryMonthlyGiftinessScores": DEFAULT_MONTHLY_SCORES_LIST,
}

RIVULET_COUNT_FEATURES = {
    "candidateInfo.docInfo.rivuletListingInfo.counts.cartAddCountPT24hFV1": 0,
    "candidateInfo.docInfo.rivuletListingInfo.counts.favoriteCountPT24hFV1": 0,
    "candidateInfo.docInfo.rivuletListingInfo.counts.purchaseCountPT168hFV1": 0,
    "candidateInfo.docInfo.rivuletListingInfo.counts.viewCountPT24hFV1": 0,
}

RIVULET_TIMESERIES_FEATURES = {
    "candidateInfo.docInfo.rivuletListingInfo.timeseries.recentlyCartaddedUserIds50FV1#userId": [],
    "candidateInfo.docInfo.rivuletListingInfo.timeseries.recentlyFavoritedUserIds50FV1#userId": [],
    "candidateInfo.docInfo.rivuletListingInfo.timeseries.recentlyPurchasedUserIds50FV1#userId": [],
    "candidateInfo.docInfo.rivuletListingInfo.timeseries.recentlyViewedUserIds50FV1#userId": [],
    "contextualInfo[name=browser].rivuletBrowserInfo.timeseries.recentlyCartaddedListingIds50FV1#listingId": [],
    "contextualInfo[name=browser].rivuletBrowserInfo.timeseries.recentlySearchedQueries50FV1#searchQuery": [],
    "contextualInfo[name=browser].rivuletBrowserInfo.timeseries.recentlyViewedListingIds50FV1#listingId": [],
    "contextualInfo[name=user].rivuletUserInfo.timeseries.recentlyCartaddedListingIds50FV1#listingId": [],
    "contextualInfo[name=user].rivuletUserInfo.timeseries.recentlyCartaddedTaxoIds50FV1#taxoId": [],
    "contextualInfo[name=user].rivuletUserInfo.timeseries.recentlyFavoritedListingIds50FV1#listingId": [],
    "contextualInfo[name=user].rivuletUserInfo.timeseries.recentlyFavoritedTaxoIds50FV1#taxoId": [],
    "contextualInfo[name=user].rivuletUserInfo.timeseries.recentlyPurchasedFromShopIds50FV1#shopId": [],
    "contextualInfo[name=user].rivuletUserInfo.timeseries.recentlyPurchasedListingIds50FV1#listingId": [],
    "contextualInfo[name=user].rivuletUserInfo.timeseries.recentlySearchedQueries50FV1#searchQuery": [],
    "contextualInfo[name=user].rivuletUserInfo.timeseries.recentlySearchedTaxoIds50FV1#taxoId": [],
    "contextualInfo[name=user].rivuletUserInfo.timeseries.recentlyViewedListingIds50FV1#listingId": [],
    "contextualInfo[name=user].rivuletUserInfo.timeseries.recentlyViewedListingShopIds50FV1#shopId": [],
    "contextualInfo[name=user].rivuletUserInfo.timeseries.recentlyViewedShopIds50FV1#shopId": [],
    "contextualInfo[name=user].rivuletUserInfo.timeseries.recentlyViewedTaxoIds50FV1#taxoId": [],
}

LABELS = {"attributions": []}

LABEL_FEATURE = "attributions_score"

SAMPLE_WEIGHT_FEATURE = "sample_weight"

SCORE_FEATURES = {"candidateInfo.scores.score": UNDEFINED_FLOAT}


def _create_raw_feature_spec() -> (
    Dict[str, Union[tf.io.FixedLenFeature, tf.io.VarLenFeature]]
):
    """Defines all features that may be parsed and used in preprocessing/serving.
    Features must also be defined in _create_raw_defaults() to handle the case of missing features.
    Features must be used within preprocess_fit.preprocess to be added to the TFT graph and used for training.
    """
    feature_spec = {}

    for feature in TEXT_FEATURES:
        feature_spec[feature] = tf.io.FixedLenFeature([], tf.string)
    for feature in FLOAT_FEATURES:
        feature_spec[feature] = tf.io.FixedLenFeature([], tf.float32)
    for feature in INT_FEATURES:
        feature_spec[feature] = tf.io.FixedLenFeature([], tf.int64)
    for feature in BOOL_FEATURES:
        feature_spec[feature] = tf.io.FixedLenFeature([], tf.int64)
    for feature, default_value in DENSE_VECTOR_FEATURES.items():
        feature_spec[feature] = tf.io.FixedLenFeature([len(default_value)], tf.float32)
    for feature in STRING_LIST_FEATURES:
        feature_spec[feature] = tf.io.VarLenFeature(tf.string)
    for feature in INT_LIST_FEATURES:
        feature_spec[feature] = tf.io.VarLenFeature(tf.int64)
    for feature in ID_FEATURES:
        feature_spec[feature] = tf.io.FixedLenFeature([], tf.int64)
    for feature in RIVULET_COUNT_FEATURES:
        feature_spec[feature] = tf.io.FixedLenFeature([], tf.int64)
    for feature in RIVULET_TIMESERIES_FEATURES:
        if any(suffix in feature for suffix in ["#searchQuery"]):
            feature_spec[feature] = tf.io.VarLenFeature(tf.string)
        elif any(
            # suffix in feature for suffix in ["#userId", "#taxoId", "#shopId", "#listingId", "#timestamp"] # TODO: uncomment timestamp after frontfill is done. timestamp is a mix of string and int for now.
            suffix in feature
            for suffix in ["#userId", "#taxoId", "#shopId", "#listingId"]
        ):
            feature_spec[feature] = tf.io.VarLenFeature(tf.int64)

    for feature in LABELS:
        feature_spec[feature] = tf.io.VarLenFeature(tf.string)

    for feature in SCORE_FEATURES:
        feature_spec[feature] = tf.io.FixedLenFeature([], tf.float32)

    for feature in VPCG_KEYS_FEATURES:
        feature_spec[feature] = tf.io.VarLenFeature(tf.string)

    for feature in VPCG_VALUES_FEATURES:
        feature_spec[feature] = tf.io.VarLenFeature(tf.float32)

    return feature_spec


def _create_raw_defaults() -> Dict[str, Union[List, int, float, str]]:
    """Creates default value dictionary for parsing missing raw features in training and serving."""
    all_feature_groups = [
        BOOL_FEATURES,
        INT_FEATURES,
        FLOAT_FEATURES,
        TEXT_FEATURES,
        ID_FEATURES,
        STRING_LIST_FEATURES,
        VPCG_KEYS_FEATURES,
        VPCG_VALUES_FEATURES,
        DENSE_VECTOR_FEATURES,
        RIVULET_COUNT_FEATURES,
        RIVULET_TIMESERIES_FEATURES,
        LABELS,
        SCORE_FEATURES,
    ]
    raw_defaults = {}
    for feature_group in all_feature_groups:
        for feature, default_value in feature_group.items():
            if feature not in raw_defaults:
                raw_defaults[feature] = default_value
            else:
                raise ValueError(
                    f"Feature {feature} is defined in multiple feature groups. Remove duplicate of this feature in schema"
                )
    return raw_defaults


def _get_feature_name_from_transformed_spec(transformed_feature_spec):
    """Given a transformed feature spec dictionary, returns list of example,context feature names for tf.SequenceExample format."""

    example_feature_names = [
        feature
        for feature in transformed_feature_spec.keys()
        if ("query_embed" not in feature)
        and ("contextualInfo[name=user].userInfo" not in feature)
        and ("contextualInfo[name=target].docInfo.queryInfo" not in feature)
        and ("contextualInfo[name=target].contextualInfo[name=target]" not in feature)
        and ("contextualInfo[name=user].contextualInfo[name=user]" not in feature)
        and ("contextualInfo[name=target].contextualInfo[name=user]" not in feature)
        and ("contextualInfo[name=user].contextualInfo[name=target]" not in feature)
        and ("contextualInfo[name=browser].rivuletBrowserInfo" not in feature)
        and ("contextualInfo[name=user].rivuletUserInfo" not in feature)
        and ("clientProvidedInfo" not in feature)
    ]

    context_feature_names = list(
        set(transformed_feature_spec.keys()) - set(example_feature_names)
    )

    return example_feature_names, context_feature_names


def _get_ec_specs(transformed_feature_spec):
    """
    Given feature_specs, returns example and context specs.
    """
    # get list of context, example feature names
    (
        example_feature_names,
        context_feature_names,
    ) = _get_feature_name_from_transformed_spec(transformed_feature_spec)

    # filter specs into dicts based on feature names
    filterByKey = lambda keys: {
        feature: transformed_feature_spec[feature] for feature in keys
    }

    example_feature_spec = filterByKey(example_feature_names)
    context_feature_spec = filterByKey(context_feature_names)

    return example_feature_spec, context_feature_spec


def flatten_json(nested_dict, partitions=None):
    """Flatten a nested dict object into a flat dict; key is concatenated by dot hierarchically.

    Args:
        nested_dict: a nested dict including all the input features hierarchically
    """
    out = {}
    vpcg_features = [i.replace("#keys", "") for i in VPCG_KEYS_FEATURES]

    def flatten(x, name=""):
        """Helper function for flattening nested dict.

        Args:
            x: object to be flattened
            name: up-to-date flattened feature name concatenated by dot
        """
        if type(x) is dict:
            for a in x:
                if name + a in vpcg_features:
                    out[name + a] = x[a]
                else:
                    flatten(x[a], name + a + ".")
        elif type(x) is list and name[:-1] == "candidateInfo.scores":
            for a in x:
                if "score" in a:
                    flatten(a["score"], "candidateInfo.scores.score.")
        elif type(x) is list and name[:-1] == "contextualInfo":
            for a in x:
                if (a["name"] == "target") and ("docInfo" in a):
                    flatten(a["docInfo"], "contextualInfo[name=target].docInfo.")
                if (a["name"] == "user") and ("userInfo" in a):
                    flatten(a["userInfo"], "contextualInfo[name=user].userInfo.")
                if (a["name"] == "user") and ("rivuletUserInfo" in a):
                    flatten(
                        a["rivuletUserInfo"],
                        "contextualInfo[name=user].rivuletUserInfo.",
                    )
                if (a["name"] == "browser") and ("rivuletBrowserInfo" in a):
                    flatten(
                        a["rivuletBrowserInfo"],
                        "contextualInfo[name=browser].rivuletBrowserInfo.",
                    )
        else:
            out[name[:-1]] = x

    flatten(nested_dict)
    raw_defaults = _create_raw_defaults()

    feature_dict = {}
    for feature_i in BOOL_FEATURES:
        if feature_i not in out:
            feature_dict[feature_i] = raw_defaults[feature_i]
        else:
            feature_dict[feature_i] = int(out[feature_i])

    for feature_i in INT_FEATURES:
        if feature_i not in out:
            feature_dict[feature_i] = raw_defaults[feature_i]
        else:
            feature_dict[feature_i] = int(out[feature_i])

    for feature_i in FLOAT_FEATURES:
        if feature_i not in out:
            feature_dict[feature_i] = raw_defaults[feature_i]
        else:
            feature_dict[feature_i] = float(out[feature_i])

    for feature_i in TEXT_FEATURES:
        if feature_i not in out:
            feature_dict[feature_i] = raw_defaults[feature_i]
        else:
            feature_dict[feature_i] = str(out[feature_i])

    for feature_i in ID_FEATURES:
        if feature_i not in out:
            feature_dict[feature_i] = raw_defaults[feature_i]
        else:
            feature_dict[feature_i] = int(out[feature_i])

    for feature_i in STRING_LIST_FEATURES:
        if feature_i not in out:
            feature_dict[feature_i] = raw_defaults[feature_i]
        else:
            feature_dict[feature_i] = [str(i) for i in out[feature_i]]

    for feature_i in INT_LIST_FEATURES:
        if feature_i not in out:
            feature_dict[feature_i] = []
        else:
            feature_dict[feature_i] = [int(i) for i in out[feature_i]]

    for feature_i in DENSE_VECTOR_FEATURES:
        if feature_i not in out:
            feature_dict[feature_i] = raw_defaults[feature_i]
        else:
            feature_dict[feature_i] = [float(i) for i in out[feature_i]]

    for feature_i in RIVULET_COUNT_FEATURES:
        if feature_i not in out:
            feature_dict[feature_i] = raw_defaults[feature_i]
        else:
            feature_dict[feature_i] = int(out[feature_i])

    for feature_i in RIVULET_TIMESERIES_FEATURES:
        if feature_i.split("#")[0] not in out:
            feature_dict[feature_i] = raw_defaults[feature_i]
        else:
            feature_dict[feature_i] = [
                rivulet_dict[feature_i.split("#")[1]]
                for rivulet_dict in out[feature_i.split("#")[0]]
            ]

    for feature_i in LABELS:
        if feature_i not in out:
            feature_dict[feature_i] = raw_defaults[feature_i]
        else:
            feature_dict[feature_i] = [str(i) for i in out[feature_i]]

    for feature_i in SCORE_FEATURES:
        if feature_i not in out:
            feature_dict[feature_i] = raw_defaults[feature_i]
        else:
            feature_dict[feature_i] = float(out[feature_i])

    if partitions:
        for feature_i in partitions:
            if feature_i in out:
                feature_dict[feature_i] = out[feature_i]

    for vpcg_i in vpcg_features:
        feature_dict[vpcg_i + "#keys"] = []
        feature_dict[vpcg_i + "#values"] = []
        if vpcg_i in out:
            sorted_vpcg_i = sorted(
                out[vpcg_i].items(), key=lambda x: x[1], reverse=True
            )
            for key, value in list(sorted_vpcg_i):
                feature_dict[vpcg_i + "#keys"].append(str(key))
                feature_dict[vpcg_i + "#values"].append(float(value))

    return feature_dict


def _create_feature_spec(
    tft_model_path: str,
) -> Tuple[
    Dict[str, tf.io.FixedLenFeature],
    Dict[str, tf.io.FixedLenFeature],
    Tuple[str, tf.io.FixedLenFeature],
    Tuple[str, tf.io.FixedLenFeature],
]:
    """Create context and example feature spec for data parsing.
    Returns:
        (context feature specs, example feature specs, label spec).
    """
    tft_output = tft.TFTransformOutput(tft_model_path)
    tft_feature_spec = tft_output.transformed_feature_spec()

    # remove raw requestUUID from features before training
    tft_feature_spec.pop("requestUUID")

    # label feature exists in features
    raw_label_feature_spec = tft_feature_spec.pop(LABEL_FEATURE)
    # set new shape
    label_feature_spec = tf.io.FixedLenFeature(
        shape=(1,), dtype=raw_label_feature_spec.dtype
    )
    label_spec = (LABEL_FEATURE, label_feature_spec)

    # build sample_weight_spec
    if SAMPLE_WEIGHT_FEATURE in tft_feature_spec:
        raw_sample_weight_feature_spec = tft_feature_spec.pop(SAMPLE_WEIGHT_FEATURE)

        sample_weight_feature_spec = tf.io.FixedLenFeature(
            shape=(1,), dtype=raw_sample_weight_feature_spec.dtype
        )
        sample_weight_spec = (SAMPLE_WEIGHT_FEATURE, sample_weight_feature_spec)
    else:
        sample_weight_spec = None

    example_feature_spec, context_feature_spec = _get_ec_specs(tft_feature_spec)

    for feature, tf_info in example_feature_spec.items():
        example_feature_spec[feature] = tf.io.FixedLenFeature(
            tf_info.shape, tf_info.dtype
        )
    for feature, tf_info in context_feature_spec.items():
        context_feature_spec[feature] = tf.io.FixedLenFeature(
            shape=tf_info.shape, dtype=tf_info.dtype
        )

    return context_feature_spec, example_feature_spec, label_spec, sample_weight_spec


def process(grouped_docs: List[Dict], raw_feature_spec, tft_layer):
    # Batch together features from each listing into lists of feature values
    tft_batch_values = {}
    for listing in grouped_docs:
        # Flatten JSON and replace all missing values
        flat_listing_dict = flatten_json(listing)
        for name, feature in raw_feature_spec.items():
            if isinstance(feature, tf.io.VarLenFeature):
                field_batch = tft_batch_values.get(name, ([], []))
                field_batch[0].extend(flat_listing_dict[name])
                field_batch[1].append(len(flat_listing_dict[name]))
                tft_batch_values[name] = field_batch
            else:
                field_batch = tft_batch_values.get(name, [])
                field_batch.append(flat_listing_dict[name])
                tft_batch_values[name] = field_batch

    # Convert lists of feature values into tensors
    tft_input = {}
    for name, feature in raw_feature_spec.items():
        if isinstance(feature, tf.io.VarLenFeature):
            values, row_lengths = tft_batch_values[name]
            tft_input[name] = tf.RaggedTensor.from_row_lengths(
                values=tf.convert_to_tensor(values, dtype=feature.dtype),
                row_lengths=row_lengths,
                validate=False,
            ).to_sparse()
        else:
            tft_input[name] = tf.constant(tft_batch_values[name], feature.dtype)

    # Tensor transformation using preprocess graph
    transformed = tft_layer(tft_input)
    return [transformed]
