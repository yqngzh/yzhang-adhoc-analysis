import logging
import time
import argparse
import numpy as np
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.internal.clients.bigquery import TableSchema
from apache_beam.io.gcp.internal.clients.bigquery import TableFieldSchema
from google.cloud import bigquery


####  Change values
FILEPATH = "gs://etldata-prod-search-ranking-data-hkwv8r/user/yzhang/listing_signals/feature_logging_training_data_parquet/query_pipeline_web_organic/tight_purchase/_DATE=2024-01-20/parquet/part-*.parquet"
RUN_ON_DATAFLOW = False
####


class ListingSignalDataProcess(beam.DoFn):
    def process(self, request):
        try:
            requestUUIDs = request["requestUUID"]
            for i in range(len(requestUUIDs)):
                out_data = {
                    "requestUUID": requestUUIDs[i],
                    "client_query": request["clientProvidedInfo.query.query"][i],
                    "listing_id": request[
                        "candidateInfo.docInfo.listingInfo.listingId"
                    ][i],
                    "lw_quantity": request[
                        "candidateInfo.docInfo.listingInfo.listingWeb.quantity"
                    ][i],
                    "lw_is_limited_quantity": int(
                        request[
                            "candidateInfo.docInfo.listingInfo.listingWeb.isLimitedQuantity"
                        ][i]
                    ),
                    "active_listing_basics_quantity": request[
                        "candidateInfo.docInfo.listingInfo.activeListingBasics.quantity"
                    ][i],
                    "lw_tags": request[
                        "candidateInfo.docInfo.listingInfo.listingWeb.tags"
                    ][i],
                    "vertical_listing_tags": request[
                        "candidateInfo.docInfo.listingInfo.verticaListings.tags"
                    ][i],
                }

                # price
                listing_web_price_keys = request[
                    "candidateInfo.docInfo.listingInfo.listingWeb.price#keys"
                ][i]
                listing_web_price_values = request[
                    "candidateInfo.docInfo.listingInfo.listingWeb.price#values"
                ][i]
                listing_web_price = 0
                for j in range(len(listing_web_price_keys)):
                    if listing_web_price_keys[j] == "US":
                        listing_web_price = listing_web_price_values[j]
                out_data["lw_price_us"] = listing_web_price
                out_data["active_listing_basics_price"] = request[
                    "candidateInfo.docInfo.listingInfo.activeListingBasics.priceUsd"
                ][i]

                # is free shipping
                is_free_shipping_tuples = request[
                    "candidateInfo.docInfo.listingInfo.listingWeb.isFreeShipping"
                ][i]
                listing_web_free_shipping = 0
                for k in range(len(is_free_shipping_tuples)):
                    if is_free_shipping_tuples[k][0] == "US":
                        listing_web_free_shipping = int(is_free_shipping_tuples[k][1])
                out_data["lw_is_free_shipping"] = listing_web_free_shipping
                out_data["us_ship_cost"] = request[
                    "candidateInfo.docInfo.listingInfo.activeListingShippingCosts.usShipCost"
                ][i]

                return [out_data]
        except:
            logging.info(f"Failed to read in: {np.unique(request['requestUUID'])}")


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output_table",
        required=False,
    )
    parser.add_argument(
        "--job_name",
        required=False,
    )
    args, _ = parser.parse_known_args(argv)

    if RUN_ON_DATAFLOW:
        now = str(int(time.time()))
        pipeline_options = PipelineOptions(
            save_main_session=True,
            pipeline_type_check=True,
            job_name=f"{args.job_name}-{now}",
        )
    else:
        pipeline_options = None

    # client = bigquery.Client(project=pipeline_options.get_all_options()["project"])
    output_schema_bq = [
        bigquery.SchemaField("requestUUID", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("client_query", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("listing_id", bigquery.enums.SqlTypeNames.INT64),
        bigquery.SchemaField("lw_quantity", bigquery.enums.SqlTypeNames.INT64),
        bigquery.SchemaField(
            "lw_is_limited_quantity", bigquery.enums.SqlTypeNames.INT64
        ),
        bigquery.SchemaField(
            "active_listing_basics_quantity", bigquery.enums.SqlTypeNames.INT64
        ),
        bigquery.SchemaField("lw_tags", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField(
            "vertical_listing_tags", bigquery.enums.SqlTypeNames.STRING
        ),
        bigquery.SchemaField("lw_price_us", bigquery.enums.SqlTypeNames.INT64),
        bigquery.SchemaField(
            "active_listing_basics_price", bigquery.enums.SqlTypeNames.FLOAT64
        ),
        bigquery.SchemaField("lw_is_free_shipping", bigquery.enums.SqlTypeNames.INT64),
        bigquery.SchemaField("us_ship_cost", bigquery.enums.SqlTypeNames.FLOAT64),
    ]
    # out_table = bigquery.Table(
    #     args.output_table.replace(":", "."), schema=output_schema_bq
    # )
    # client.create_table(out_table, exists_ok=True)

    output_schema_beam = TableSchema()
    for item in output_schema_bq:
        field_schema = TableFieldSchema()
        field_schema.name = item.name
        field_schema.type = item.field_type
        field_schema.mode = item.mode
        output_schema_beam.fields.append(field_schema)

    with beam.Pipeline(options=pipeline_options) as pipeline:
        (
            pipeline
            | "CreateFileList" >> beam.Create([FILEPATH])
            | "ReadFiles" >> beam.io.ReadAllFromParquet()
            | "GetNeededFields" >> beam.ParDo(ListingSignalDataProcess())
            | "Print" >> beam.Map(print)
            # | "Write results to BigQuery"
            # >> beam.io.WriteToBigQuery(
            #     args.output_table,
            #     schema=output_schema_beam,
            #     method="FILE_LOADS",
            # )
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
