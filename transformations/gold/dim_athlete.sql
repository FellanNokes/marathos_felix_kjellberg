CREATE OR REFRESH MATERIALIZED VIEW marathos.gold.dim_athlete
  COMMENT "Dim athlete table - gold layer" AS
SELECT
  athlete_id_hash,
  MAX_BY(athlete_id, year_of_event) AS athlete_id,
  MAX_BY(athlete_club, year_of_event) AS athlete_club,
  MAX_BY(athlete_year_of_birth, year_of_event) AS athlete_year_of_birth,
  MAX_BY(athlete_gender, year_of_event) AS athlete_gender,
  MAX_BY(athlete_age_category, year_of_event) AS athlete_age_category,
  MAX_BY(athlete_age, year_of_event) AS athlete_age,
  MAX_BY(country_name, year_of_event) AS country_name,
  MAX_BY(athlete_id_is_placeholder, year_of_event) AS athlete_id_is_placeholder
FROM
  marathos.silver.obt_marathos
WHERE
  athlete_id_hash IS NOT NULL
GROUP BY
  athlete_id_hash