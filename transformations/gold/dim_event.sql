CREATE OR REFRESH MATERIALIZED VIEW marathos.gold.dim_event
  COMMENT "Dim event table - gold layer" AS
SELECT DISTINCT
  event_id,
  event_name,
  event_distance_or_length,
  event_distance_km,
  event_duration_hours,
  distance_type,
  start_date,
  end_date
FROM
  marathos.silver.obt_marathos
WHERE
  event_id IS NOT NULL