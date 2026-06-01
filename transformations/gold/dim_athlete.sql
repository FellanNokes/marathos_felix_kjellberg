CREATE OR REFRESH MATERIALIZED VIEW marathos.gold.dim_athlete
  COMMENT "Dim athlete table - gold layer" AS
SELECT DISTINCT
  athlete_id_hash,
  athlete_id,
  athlete_club,
  athlete_year_of_birth,
  athlete_gender,
  athlete_age_category,
  country_name,
  athlete_id_is_placeholder
FROM
  marathos.silver.obt_marathos
WHERE
  athlete_id_hash IS NOT NULL