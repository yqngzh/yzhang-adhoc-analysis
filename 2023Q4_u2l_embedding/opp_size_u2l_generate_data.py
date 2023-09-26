# source: https://github.com/etsy/recs/tree/a9ab6bc6feb9d09195451c2699605c535278d25f/models/candidate_com_u2l_dual_encoder/beam_jobs/inference

import logging
import time
import argparse
from typing import List
import numpy as np
import pyarrow
import tensorflow as tf
import tensorflow_transform as tft
import tensorflow_transform.beam as tft_beam
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.internal.clients.bigquery import TableSchema
from apache_beam.io.gcp.internal.clients.bigquery import TableFieldSchema
from apache_beam.utils.shared import Shared
from google.cloud import bigquery


def fill_all_other_features(data, input_spec):
    ###get some smart defaults in here
    defaults_by_dtype = {
        tf.int64: 0,
        tf.float32: 0.0,
        tf.string: "0",
    }
    data = {k: v for k, v in data.items() if k in input_spec}
    data = {
        k: v if v is not None else defaults_by_dtype[input_spec[k].dtype]
        for k, v in data.items()
    }
    data.update(
        {
            k: defaults_by_dtype[input_spec[k].dtype]
            for k in set(input_spec.keys()).difference(set(data.keys()))
        }
    )
    return data


class UserEncoderPredict(beam.DoFn):
    def __init__(self, shared_handle, saved_model_path):
        super().__init__()
        self._saved_model_path = saved_model_path
        self._shared_handle = shared_handle

    def setup(self):
        def initialize_model():
            return tf.saved_model.load(self._saved_model_path)

        self._model = self._shared_handle.acquire(initialize_model)

    def process(self, data):
        signature_key = "serving_default"
        predictor = self._model.signatures[signature_key]
        model_output = predictor(tf.constant([data]))
        user_embedding = model_output["user_embeddings"]
        user_id = model_output["user_id"].numpy().tolist()[0]
        embedding = user_embedding.numpy().tolist()[0]
        yield {"id": user_id, "embedding": embedding}


def userPredictor(inputs, user_coder, shared_handle, options):
    user_tower_output = (
        inputs
        | "Encode User Predict Data" >> beam.Map(user_coder.encode)
        | "Predict user"
        >> beam.ParDo(UserEncoderPredict(shared_handle, options.user_model_path))
    )
    return user_tower_output


def get_output_schema():
    return pyarrow.schema(
        [("id", pyarrow.int64()), ("embedding", pyarrow.list_(pyarrow.float32()))]
    )


def configure_pipeline(pipeline, options):
    with tft_beam.Context(options.transform_location):
        tf_transform_output = tft.TFTransformOutput(options.transform_fn_path)
        inputSpec = tf_transform_output.raw_feature_spec()
        input_data = (
            pipeline
            | beam.io.ReadFromParquet(options.input_files_path)
            | "fill all other user features with empty string"
            >> beam.Map(fill_all_other_features, inputSpec)
        )

        user_coder = tft.coders.ExampleProtoCoder(
            tf_transform_output.raw_metadata.schema
        )
        shared_handle = Shared()
        user_tower_output = userPredictor(
            input_data, user_coder, shared_handle, options
        )
        user_tower_output | "Write User Embeddings" >> beam.io.WriteToParquet(
            options.output_path, get_output_schema()
        )


class UserInferenceOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            "--input_files_path",
            required=True,
            help="Path to user inference data, should come from spark",
        )
        parser.add_argument(
            "--output_path",
            required=True,
            help="Output directory to write results to.",
        )
        parser.add_argument("--transform_location")
        parser.add_argument(
            "--user_model_path",
            required=True,
            help="Pretrained User model Directory, should come from tensorflow",
        )
        parser.add_argument(
            "--transform_fn_path",
            required=True,
            help="Path of TFT output, should come from beam",
        )


def main():
    """Runs the pipeline."""
    pipeline_options = PipelineOptions(pipeline_type_check=True)
    user_options = pipeline_options.view_as(UserInferenceOptions)
    with beam.Pipeline(options=pipeline_options) as p:
        configure_pipeline(p, user_options)


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_table",
        required=True,
        help="Input table with requests and embeddings",
    )
    parser.add_argument(
        "--output_table",
        required=True,
        help="Output table to write results to",
    )
    parser.add_argument(
        "--output_dir",
        required=True,
        help="Output file to write results to",
    )
    args, _ = parser.parse_known_args(argv)

    now = str(int(time.time()))
    pipeline_options = PipelineOptions(
        save_main_session=True,
        pipeline_type_check=True,
        job_name=f"yzhang-visual-diversity-embedding-process-{now}",
    )

    output_schema_beam = TableSchema()

    field_schema = TableFieldSchema()
    field_schema.name = "requestUUID"
    field_schema.type = "STRING"
    field_schema.mode = "NULLABLE"
    output_schema_beam.fields.append(field_schema)

    field_schema = TableFieldSchema()
    field_schema.name = "visitId"
    field_schema.type = "STRING"
    field_schema.mode = "NULLABLE"
    output_schema_beam.fields.append(field_schema)

    field_schema = TableFieldSchema()
    field_schema.name = "query_str"
    field_schema.type = "STRING"
    field_schema.mode = "NULLABLE"
    output_schema_beam.fields.append(field_schema)

    field_schema = TableFieldSchema()
    field_schema.name = "query_bin"
    field_schema.type = "STRING"
    field_schema.mode = "NULLABLE"
    output_schema_beam.fields.append(field_schema)

    field_schema = TableFieldSchema()
    field_schema.name = "listing_ids"
    field_schema.type = "INT64"
    field_schema.mode = "REPEATED"
    output_schema_beam.fields.append(field_schema)

    field_schema = TableFieldSchema()
    field_schema.name = "cosine_sim"
    field_schema.type = "FLOAT64"
    field_schema.mode = "REPEATED"
    output_schema_beam.fields.append(field_schema)

    output_schema_bq = [
        bigquery.SchemaField("requestUUID", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("visitId", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("query_str", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("query_bin", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField(
            "listing_ids", bigquery.enums.SqlTypeNames.INT64, mode="REPEATED"
        ),
        bigquery.SchemaField(
            "cosine_sim", bigquery.enums.SqlTypeNames.FLOAT64, mode="REPEATED"
        ),
    ]

    table = bigquery.Table(args.output_table.replace(":", "."), schema=output_schema_bq)
    client = bigquery.Client(project=pipeline_options.get_all_options()["project"])
    client.create_table(table, exists_ok=True)

    with beam.Pipeline(options=pipeline_options) as pipeline:
        # with beam.Pipeline() as pipeline:
        (
            pipeline
            # | "Create" >> beam.Create(input_data)
            | "Read input data"
            >> beam.io.ReadFromBigQuery(
                query=f"select * from `{args.input_table}` order by requestUUID, position",
                use_standard_sql=True,
                gcs_location=f"gs://etldata-prod-search-ranking-data-hkwv8r/data/shared/tmp",
            )
            | "Group by request" >> beam.GroupBy(lambda x: x["requestUUID"])
            | "Get embedding similarity" >> beam.ParDo(GetEmbeddingAndSimilarity())
            # | "Print" >> beam.Map(print)
            | "Write results to BigQuery"
            >> beam.io.WriteToBigQuery(
                args.output_table,
                schema=output_schema_beam,
                method="FILE_LOADS",
            )
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
