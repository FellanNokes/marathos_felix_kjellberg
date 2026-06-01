CREATE OR REFRESH MATERIALIZED VIEW marathos.gold.dim_date
  COMMENT "Dim date table - gold layer" AS
SELECT DISTINCT
  start_date AS date,
  YEAR(start_date) AS year,
  MONTH(start_date) AS month,
  DATE_FORMAT(start_date, 'MMMM') AS month_name,
  DAY(start_date) AS day,
  date_format(start_date, 'EEEE') AS day_name,
  DAYOFWEEK(start_date) AS day_of_week,
  QUARTER(start_date) AS quarter,
  CASE
    WHEN DAYOFWEEK(start_date) in (1, 7) THEN true
    ELSE false
  END AS is_weekend
FROM
  marathos.silver.obt_marathos
WHERE
  start_date IS NOT NULL