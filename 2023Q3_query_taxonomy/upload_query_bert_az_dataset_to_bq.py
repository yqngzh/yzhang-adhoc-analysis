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
        job_name=f"yzhang-taxo-boost-ab-analysis-{now}",
    )

    client = bigquery.Client(project=pipeline_options.get_all_options()["project"])
    in_table = client.get_table(args.input_table)
    in_schema_bq = in_table.schema
    in_schema_bq_reduced = [
        item
        for item in in_schema_bq
        if item.name
        not in [
            "click_top_taxo",
            "click_level2_taxo",
            "purchase_top_taxo",
            "purchase_level2_taxo",
        ]
    ]
    output_schema_bq = in_schema_bq_reduced + [
        bigquery.SchemaField("click_top_overlap", bigquery.enums.SqlTypeNames.INT64),
        bigquery.SchemaField("purchase_top_overlap", bigquery.enums.SqlTypeNames.INT64),
        bigquery.SchemaField("click_level2_overlap", bigquery.enums.SqlTypeNames.INT64),
        bigquery.SchemaField(
            "purchase_level2_overlap", bigquery.enums.SqlTypeNames.INT64
        ),
    ]
    out_table = bigquery.Table(
        args.output_table.replace(":", "."), schema=output_schema_bq
    )
    client.create_table(out_table, exists_ok=True)

    output_schema_beam = TableSchema()
    for item in output_schema_bq:
        field_schema = TableFieldSchema()
        field_schema.name = item.name
        field_schema.type = item.field_type
        field_schema.mode = item.mode
        output_schema_beam.fields.append(field_schema)

    with beam.Pipeline(options=pipeline_options) as pipeline:
        # with beam.Pipeline() as pipeline:
        (
            pipeline
            # | "Create" >> beam.Create(input_data)
            | "Read input data"
            >> beam.io.ReadFromBigQuery(
                query=f"select * from `{args.input_table}`",
                # query="select * from `etsy-sr-etl-prod.yzhang.query_taxo_web_full` limit 100",
                use_standard_sql=True,
                gcs_location=f"gs://etldata-prod-search-ranking-data-hkwv8r/data/shared/tmp",
            )
            # | "Print" >> beam.Map(print)
            | "Data summary" >> beam.ParDo(DemandProcess())
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


print_row(row)