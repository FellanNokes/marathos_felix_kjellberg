USE CATALOG marathos;

USE SCHEMA gold;

CREATE OR REFRESH MATERIALIZED VIEW marathos.gold.mart_time_event
  COMMENT "Mart for events that are distance - Gold layer" AS
SELECT
  re.distance_type,
  re.performance_distance,
  re.average_speed,
  re.event_number_of_finishers,
  re.year_of_event,
  e.event_name,
  e.event_distance_km,
  e.start_date,
  e.end_date,
  re.year_of_event - a.athlete_year_of_birth AS athlete_age,
  a.athlete_gender,
  a.country_name
FROM
  fct_results re
    LEFT JOIN dim_event e
      ON re.event_id = e.event_id
    LEFT JOIN dim_athlete a
      ON re.athlete_id_hash = a.athlete_id_hash
WHERE
  re.distance_type = 'time';