CREATE OR REFRESH STREAMING TABLE marathos.bronze.country_codes
    COMMENT "Country codes - bronze layer" AS
    SELECT
        *
    FROM
        STREAM read_files(
            "/Volumes/marathos/default/raw/country_codes/",
            format => "csv",
            header => "true",
            inferSchema => "true"
        )