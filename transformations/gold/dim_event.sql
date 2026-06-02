CREATE OR REFRESH MATERIALIZED VIEW marathos.gold.dim_event
  COMMENT "Dim event table - gold layer" AS
SELECT
  event_id,
  MAX_BY(event_name, year_of_event) AS event_name,
  MAX_BY(event_distance_or_length, year_of_event) AS event_distance_or_length,
  MAX_BY(event_distance_km, year_of_event) AS event_distance_km,
  MAX_BY(event_duration_hours, year_of_event) AS event_duration_hours,
  MAX_BY(distance_type, year_of_event) AS distance_type,
  MAX_BY(start_date, year_of_event) AS start_date,
  MAX_BY(end_date, year_of_event) AS end_date
FROM
  marathos.silver.obt_marathos
WHERE
  event_id IS NOT NULL
GROUP BY
  event_id