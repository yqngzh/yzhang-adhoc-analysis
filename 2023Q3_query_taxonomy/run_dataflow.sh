python -m analyze_taxo_boosting_ab \
    --input_table etsy-sr-etl-prod.yzhang.query_taxo_boe_full \
    --output_table etsy-sr-etl-prod:yzhang.query_taxo_boe_summary \
    --output_dir gs://etldata-prod-search-ranking-data-hkwv8r/data/shared/tmp \
    --runner DataflowRunner \
    --project etsy-sr-etl-prod \
    --region us-central1 \
    --service_account_email dataflow-worker@etsy-sr-etl-prod.iam.gserviceaccount.com \
    --temp_location gs://etldata-prod-search-ranking-data-hkwv8r/data/shared/tmp \
    --staging_location gs://etldata-prod-search-ranking-data-hkwv8r/data/shared/tmp \
    --experiment use_runner_v2 \
    --experiment upload_graph \
    --experiment max_workflow_runtime_walltime_seconds=43200 \
    --machine_type e2-standard-16 \
    --disk_size_gb 200 \
    --num_workers 16 \
    --max_num_workers 32


python -m distrib_match_opp_sizing \
    --input_table etsy-sr-etl-prod.yzhang.query_taxo_distrib_match_clean_2023_09_24 \
    --output_table etsy-sr-etl-prod:yzhang.query_taxo_distrib_match_2023_09_24_summary \
    --output_dir gs://etldata-prod-search-ranking-data-hkwv8r/data/shared/tmp \
    --runner DataflowRunner \
    --project etsy-sr-etl-prod \
    --region us-central1 \
    --service_account_email dataflow-worker@etsy-sr-etl-prod.iam.gserviceaccount.com \
    --temp_location gs://etldata-prod-search-ranking-data-hkwv8r/data/shared/tmp \
    --staging_location gs://etldata-prod-search-ranking-data-hkwv8r/data/shared/tmp \
    --experiment use_runner_v2 \
    --experiment upload_graph \
    --experiment max_workflow_runtime_walltime_seconds=43200 \
    --machine_type e2-standard-16 \
    --disk_size_gb 200 \
    --num_workers 16 \
    --max_num_workers 32


python -m last_pass_boost_parameter \
    --input_table etsy-sr-etl-prod.yzhang.query_taxo_lastpass_rpc \
    --output_table etsy-sr-etl-prod:yzhang.query_taxo_lastpass_rpc_analysis \
    --output_dir gs://etldata-prod-search-ranking-data-hkwv8r/data/shared/tmp \
    --runner DataflowRunner \
    --project etsy-sr-etl-prod \
    --region us-central1 \
    --service_account_email dataflow-worker@etsy-sr-etl-prod.iam.gserviceaccount.com \
    --temp_location gs://etldata-prod-search-ranking-data-hkwv8r/data/shared/tmp \
    --staging_location gs://etldata-prod-search-ranking-data-hkwv8r/data/shared/tmp \
    --experiment use_runner_v2 \
    --experiment upload_graph \
    --experiment max_workflow_runtime_walltime_seconds=43200 \
    --machine_type e2-standard-16 \
    --disk_size_gb 200 \
    --num_workers 16 \
    --max_num_workers 32