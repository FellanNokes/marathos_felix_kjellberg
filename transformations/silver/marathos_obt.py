from pyspark import pipelines as dp
from pyspark.sql.functions import col, when
from utils.utils import (
    rename_columns_to_snake_case,
    parse_event_dates,
    convert_none_strings_to_null,
    fill_missing_string_cols,
    create_sha2_ids,
    process_performance,
    process_event_distance
)


@dp.table(
    name="marathos.silver.obt_marathos",
    comment="Cleaned marathon data - One Big Table",
    table_properties={
        "delta.columnMapping.mode": "name",
        "delta.minReaderVersion": "2",
        "delta.minWriterVersion": "5",
    },
)
def clean_marathos():
    df = spark.sql("FROM STREAM marathos.bronze.raw_marathos")

    # Step 1 - Rename columns to snake case
    df = rename_columns_to_snake_case(df)

    # Step 2 - Convert None-strings to null
    df = convert_none_strings_to_null(df)

    # Step 3 - Cast types
    df = df.withColumn(
        "athlete_average_speed",
        when(
            col("athlete_average_speed").rlike(r"^\d+\.?\d*$"),
            col("athlete_average_speed").cast("double"),
        ).otherwise(None),
    )
    df = df.withColumn(
        "athlete_year_of_birth", col("athlete_year_of_birth").cast("int")
    )

    # Step 3 - Event dates
    df = parse_event_dates(df)

    # Step 4 - Flag placeholder IDs
    df = df.withColumn(
        "athlete_id_is_placeholder",
        when(col("athlete_id").isin(4033, 5265), True).otherwise(False),
    )

    # Step 5 - Drop duplicates
    df = df.dropDuplicates()

    # Step 6 - Clean birth year
    df = df.withColumn(
        "athlete_year_of_birth",
        when(
            (col("athlete_year_of_birth") >= col("year_of_event"))
            | (col("year_of_event") - col("athlete_year_of_birth") > 100)
            | (col("year_of_event") - col("athlete_year_of_birth") < 10),
            None,
        ).otherwise(col("athlete_year_of_birth")),
    )

    # Step 7 - Process athlete performance
    df = process_performance(df)

    # Step 8 - Process events
    df = process_event_distance(df)

    # Step 9 - Join country codes
    country_df = spark.table("marathos.bronze.country_codes")
    df = df.join(country_df, df.athlete_country == country_df.ioc_code, "left").drop(
        country_df.ioc_code
    )

    # Step 10 - Fill nulls
    df = fill_missing_string_cols(df, ["athlete_club", "athlete_gender", "athlete_age_category"])

    # Step 11 - Giving unique ids to events and athletes(some athlete ids are placeholders)
    df = create_sha2_ids(df)

    # Step 12 - Drop original columns
    return df.drop(
        "athlete_performance",
        "athlete_average_speed",
        "athlete_country",
        "event_dates",
        "is_ioc",
        "_rescued_data"
    )
