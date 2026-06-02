USE CATALOG marathos;

USE SCHEMA gold;

CREATE OR REFRESH MATERIALIZED VIEW marathos.gold.mart_general
  COMMENT "Mart for general stats - Gold layer" AS
SELECT
  re.year_of_event,
  re.distance_type,
  re.average_speed,
  re.event_number_of_finishers,
  e.event_name,
  e.event_distance_km,
  e.event_duration_hours,
  e.start_date,
  a.athlete_gender,
  a.country_name,
  re.year_of_event - a.athlete_year_of_birth AS athlete_age
FROM
  fct_results re
    LEFT JOIN dim_event e
      ON re.event_id = e.event_id
    LEFT JOIN dim_athlete a
      ON re.athlete_id_hash = a.athlete_id_hash
WHERE
  re.year_of_event - a.athlete_year_of_birth BETWEEN 10 AND 90