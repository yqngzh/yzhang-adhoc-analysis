import numpy as np

# def get_listing_taxonomy(listing_ids: List):
#     query_str = f"""
#     select listing_id, full_path
#     from `etsy-data-warehouse-prod.materialized.listing_taxonomy`
#     where listing_id in ({",".join([str(x) for x in listing_ids])})
#     """
#     df = run_query_df(query_str)
#     df["tax_top1_node"] = df.full_path.apply(
#         lambda x: x.split(".")[0] if len(x.split(".")) > 1 else x
#     )
#     df["tax_top2_node"] = df.full_path.apply(
#         lambda x: x.split(".")[1] if len(x.split(".")) > 1 else None
#     )
#     return df


def min_max_standardize(x):
    return (x - np.min(x)) / (np.max(x) - np.min(x))
