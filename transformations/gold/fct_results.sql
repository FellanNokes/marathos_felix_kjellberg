CREATE OR REFRESH STREAMING TABLE marathos.gold.fct_results
  COMMENT "Fact table - gold layer" AS
SELECT
  result_id,
  event_id,
  athlete_id_hash,
  start_date as date,
  performance_seconds,
  performance_distance,
  average_speed,
  year_of_event,
  distance_type,
  event_number_of_finishers
FROM
  STREAM marathos.silver.obt_marathos;