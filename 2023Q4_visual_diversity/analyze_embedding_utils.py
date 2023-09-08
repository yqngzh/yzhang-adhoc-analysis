from typing import List
import numpy as np
import apache_beam as beam
from google.cloud import bigquery


input_data = [
    {
        "requestUUID": "aaa",
        "position": 1,
        "query_str": "test1",
        "query_bin": "head",
        "listing_id": 725104244,
        "embedding": [{"element": 3.0}, {"element": 4.0}],
    },
    {
        "requestUUID": "aaa",
        "position": 0,
        "query_str": "test1",
        "query_bin": "head",
        "listing_id": 902792488,
        "embedding": [{"element": 1.0}, {"element": 2.0}],
    },
    {
        "requestUUID": "aaa",
        "position": 2,
        "query_str": "test1",
        "query_bin": "head",
        "listing_id": 1221932302,
        "embedding": [{"element": 5.0}, {"element": 6.0}],
    },
    {
        "requestUUID": "bbb",
        "position": 0,
        "query_str": "xxx",
        "query_bin": "top01",
        "listing_id": 793904541,
        "embedding": [{"element": 7.0}, {"element": 8.0}],
    },
    {
        "requestUUID": "bbb",
        "position": 1,
        "query_str": "xxx",
        "query_bin": "top01",
        "listing_id": 795642038,
        "embedding": None,
    },
    {
        "requestUUID": "bbb",
        "position": 2,
        "query_str": "xxx",
        "query_bin": "top01",
        "listing_id": 1323065344,
        "embedding": [],
    },
]


def run_query_df(query_str, project_id="etsy-bigquery-adhoc-prod"):
    client = bigquery.Client(project=project_id)
    query_job = client.query(query_str)
    rows = query_job.result()
    df = rows.to_dataframe()
    return df


def convert_bq_beam_schema(schema):
    output = []
    for item in schema:
        item_dict = item.to_api_repr()
        output.append(f"{item_dict['name']}:{item_dict['type']}")
    return ",".join(output)


class GetEmbeddingAndSimilarity(beam.DoFn):
    def compute_similarity(self, embeddings: List[List[float]]):
        if len(embeddings) > 0:
            embed_array = np.array(embeddings)
            data_normed = embed_array / np.linalg.norm(
                embed_array, axis=1, keepdims=True
            )
            cosine_sim_matrix = data_normed @ data_normed.T
            return list(cosine_sim_matrix.flatten())
        else:
            return []

    def process(self, row):
        in_data = row[1]
        out_data = {
            "requestUUID": in_data[0]["requestUUID"],
            "query_str": in_data[0]["query_str"],
            "query_bin": in_data[0]["query_bin"],
        }

        new_listing_ids, positions, embeddings = [], [], []
        for i in range(len(in_data)):
            curr_in_data = in_data[i]
            if (curr_in_data["embedding"] is not None) and (
                len(curr_in_data["embedding"]) > 0
            ):
                new_listing_ids.append(curr_in_data["listing_id"])
                embeddings.append(
                    [item["element"] for item in curr_in_data["embedding"]]
                )
                positions.append(curr_in_data["position"])

        # sort by position
        zipped_data = sorted(zip(positions, new_listing_ids, embeddings))
        new_listing_ids_sorted = [item[1] for item in zipped_data]
        embeddings_sorted = [item[2] for item in zipped_data]

        out_data["listing_ids"] = new_listing_ids_sorted
        out_data["cosine_sim"] = self.compute_similarity(embeddings_sorted)
        return [out_data]
