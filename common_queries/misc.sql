----  GCS Parquet to BQ  ----
CREATE OR REPLACE EXTERNAL TABLE `etsy-sr-etl-prod.yzhang.query_missing_fl_raw`
OPTIONS (
    format = 'parquet',
    uris = ['gs://ml-systems-prod-raw-mmx-logs-zjh13h/java-consumer/parquet/query_pipeline_web_organic/_DATE=2023-10-04/_HOUR=23/*.parquet']
)


----  Filter mature listings  ----
-- https://etsy.slack.com/archives/C02BEL5DXNJ/p1713555841586769?thread_ts=1713552696.855829&cid=C02BEL5DXNJ
select * from `etsy-data-warehouse-prod.etsy_aux.compliance_blocklist_terms`
where lower(query) LIKE concat('%', term.term, '%') 
and term.context = 'mature content'

WHERE term.blocklist_term_id is null

