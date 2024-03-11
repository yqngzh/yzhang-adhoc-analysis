---- Create function on dates
CREATE OR REPLACE TABLE FUNCTION `etsy-search-ml-dev.human_annotation.get_gms_by_level2_taxo`(
  endDate DATE,
  lookbackDays INT64
)
AS
  WITH taxo_gms_daily_rollup AS (
    SELECT 
      r.date, r.taxonomy_id, usd_gms_net, 
      SPLIT(full_path, '.')[SAFE_OFFSET(0)] AS top_taxo_string,
      SPLIT(full_path, '.')[SAFE_OFFSET(1)] AS level2_taxo_string
    FROM `etsy-data-warehouse-prod.rollups.gms_daily_by_taxonomy_node` r
    LEFT JOIN `etsy-data-warehouse-prod.structured_data.taxonomy` t
    ON r.taxonomy_id = t.taxonomy_id
    WHERE date BETWEEN DATE_SUB(endDate, INTERVAL lookbackDays DAY) AND endDate
  ),
  level2_taxo_gms AS (
    SELECT 
      IF (top_taxo_string IS NOT NULL AND level2_taxo_string IS NOT NULL, CONCAT(top_taxo_string, '.', level2_taxo_string), NULL) AS level2_taxo_node,
      usd_gms_net
    FROM taxo_gms_daily_rollup
  ),
  level2_taxo_gms_agg AS (
    SELECT level2_taxo_node, SUM(usd_gms_net) AS usd_gms_net
    FROM level2_taxo_gms
    WHERE level2_taxo_node IS NOT NULL
    GROUP BY level2_taxo_node
  )
  SELECT 
    level2_taxo_node, usd_gms_net,
    usd_gms_net / total AS gms_percent
  FROM level2_taxo_gms_agg, 
    (SELECT SUM(usd_gms_net) total FROM level2_taxo_gms_agg)
;


CREATE OR REPLACE TABLE FUNCTION `etsy-search-ml-dev.human_annotation.get_gms_by_top_taxo`(
  endDate DATE,
  lookbackDays INT64
)
AS
  WITH taxo_gms_daily_rollup AS (
    SELECT 
      r.date, r.taxonomy_id, usd_gms_net, 
      SPLIT(full_path, '.')[SAFE_OFFSET(0)] AS top_taxo_node,
    FROM `etsy-data-warehouse-prod.rollups.gms_daily_by_taxonomy_node` r
    LEFT JOIN `etsy-data-warehouse-prod.structured_data.taxonomy` t
    ON r.taxonomy_id = t.taxonomy_id
    WHERE date BETWEEN DATE_SUB(endDate, INTERVAL lookbackDays DAY) AND endDate
  ),
  top_taxo_gms_agg AS (
    SELECT top_taxo_node, SUM(usd_gms_net) AS usd_gms_net
    FROM taxo_gms_daily_rollup
    WHERE top_taxo_node IS NOT NULL
    GROUP BY top_taxo_node
  )
  SELECT 
    top_taxo_node, usd_gms_net,
    usd_gms_net / total AS gms_percent
  FROM top_taxo_gms_agg, 
    (SELECT SUM(usd_gms_net) total FROM top_taxo_gms_agg)
;


---- Create actual tables
CREATE OR REPLACE TABLE `etsy-search-ml-dev.human_annotation.level2_taxo_gms`
AS
SELECT * FROM `etsy-search-ml-dev.human_annotation.get_gms_by_level2_taxo`('2024-03-10', 90)
;

CREATE OR REPLACE TABLE `etsy-search-ml-dev.human_annotation.top_taxo_gms`
AS
SELECT * FROM `etsy-search-ml-dev.human_annotation.get_gms_by_top_taxo`('2024-03-10', 90)
;
