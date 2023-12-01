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
    --output_table etsy-sr-etl-prod:yzhang.query_taxo_lastpass_rpc_cutoff5\
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


python -m upload_query_bert_az_dataset_to_bq \
    --input_path gs://etldata-prod-prolist-data-hkwv8r/data/outgoing/arizona/query_classifier_prolist_3mo/20231108 \
    --output_table etsy-sr-etl-prod:yzhang.query_bert_taxo_2023_11_08 \
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


python -m last_pass_boost_parameter_bert \
    --input_table etsy-sr-etl-prod.yzhang.query_taxo_bert_lastpass_rpc \
    --output_table etsy-sr-etl-prod:yzhang.query_taxo_bert_lastpass_rpc_analysis_normalized \
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


python -m query_taxo_ancestor_generation \
    --input_table etsy-sr-etl-prod.yzhang.query_taxo_lastpass_rpc_taxo_ancenstor_raw \
    --output_table etsy-sr-etl-prod:yzhang.query_taxo_lastpass_rpc_taxo_ancenstor \
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


python -m qtd_boost \
    --input_table etsy-sr-etl-prod.yzhang.qtd_boosting_results \
    --output_table etsy-sr-etl-prod:yzhang.qtd_boosting_results_overlap \
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


python -m qtd_distribution_match \
    --input_table etsy-sr-etl-prod.yzhang.qtd_level2full_tire_sanity_raw \
    --output_table etsy-sr-etl-prod:yzhang.qtd_level2full_tire_sanity_processed \
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
#     --input_table etsy-sr-etl-prod.yzhang.qtd_distrib_match_qtdlevel2_raw \
#     --output_table etsy-sr-etl-prod:yzhang.qtd_distrib_match_qtdlevel2_processed \

#     --input_table etsy-sr-etl-prod.yzhang.qtd_distrib_match_lastab_raw \
#     --output_table etsy-sr-etl-prod:yzhang.qtd_distrib_match_lastab_processed \

# filter5 downweight
#     --input_table etsy-sr-etl-prod.yzhang.qtd_level2_tire_v2_raw \
#     --output_table etsy-sr-etl-prod:yzhang.qtd_level2_tire_v2_processed \

# filter5 dist force
#     --input_table etsy-sr-etl-prod.yzhang.qtd_level2_tire_dforce_raw \
#     --output_table etsy-sr-etl-prod:yzhang.qtd_level2_tire_dforce_processed \

# full data downweight
#     --input_table etsy-sr-etl-prod.yzhang.qtd_level2full_tire_raw \
#     --output_table etsy-sr-etl-prod:yzhang.qtd_level2full_tire_processed \

# full data dist force
#     --input_table etsy-sr-etl-prod.yzhang.qtd_level2full_tire_dforce_raw \
#     --output_table etsy-sr-etl-prod:yzhang.qtd_level2full_tire_dforce_processed \


python -m qtd_bert_distribution_match \
    --input_table etsy-sr-etl-prod.yzhang.qtd_distrib_match_bert_raw \
    --output_table etsy-sr-etl-prod:yzhang.qtd_distrib_match_bert_processed \
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
