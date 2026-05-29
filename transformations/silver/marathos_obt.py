from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col, when, to_date, regexp_extract, 
    concat, lit, round, sha2, concat_ws, monotonically_increasing_id
)
from utils.utils import rename_columns_to_snake_case


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

    string_cols = [c for c, t in df.dtypes if t == "string"]

    # Step 2 - Convert None-strings to null
    for column in string_cols:
        df = df.withColumn(
            column,
            when(col(column) == "None", None).otherwise(col(column))
        )

    # Step 3 - Cast types
    df = df.withColumn("athlete_average_speed",
        when(col("athlete_average_speed").rlike(r"^\d+\.?\d*$"),
            col("athlete_average_speed").cast("double")
        ).otherwise(None)
    )
    df = df.withColumn("athlete_year_of_birth", col("athlete_year_of_birth").cast("int"))

    # Step 3 - Event dates
    # Got help from LLM with this
    df = df.withColumn("start_date",
        when(col("event_dates").rlike(r"\d{2}\.\d{2}\.\d{4}-\d{2}\.\d{2}\.\d{4}"),
            to_date(regexp_extract(col("event_dates"), r"^(\d{2}\.\d{2}\.\d{4})", 1), "dd.MM.yyyy")
        )
        .when(col("event_dates").rlike(r"^\d{2}\.-\d{2}\.\d{2}\.\d{4}"),
            to_date(
                concat(
                    regexp_extract(col("event_dates"), r"^(\d{2})\.", 1),
                    lit("."),
                    regexp_extract(col("event_dates"), r"-\d{2}\.(\d{2}\.\d{4})$", 1)
                ),
                "dd.MM.yyyy"
            )
        )
        .otherwise(to_date(col("event_dates"), "dd.MM.yyyy"))
    ).withColumn("end_date",
        when(col("event_dates").rlike(r"\d{2}\.\d{2}\.\d{4}-\d{2}\.\d{2}\.\d{4}"),
            to_date(regexp_extract(col("event_dates"), r"(\d{2}\.\d{2}\.\d{4})$", 1), "dd.MM.yyyy")
        )
        .when(col("event_dates").rlike(r"^\d{2}\.-\d{2}\.\d{2}\.\d{4}"),
            to_date(
                concat(
                    regexp_extract(col("event_dates"), r"-(\d{2})\.\d{2}\.\d{4}$", 1),
                    lit("."),
                    regexp_extract(col("event_dates"), r"-\d{2}\.(\d{2}\.\d{4})$", 1)
                ),
                "dd.MM.yyyy"
            )
        )
        .otherwise(None)
    )

    # Step 4 - Flag placeholder IDs
    df = df.withColumn("athlete_id_is_placeholder",
        when(col("athlete_id").isin(4033, 5265), True).otherwise(False)
    )

    # Step 5 - Drop duplicates
    df = df.dropDuplicates()

    # Step 6 - Clean birth year
    df = df.withColumn("athlete_year_of_birth",
        when(
            (col("athlete_year_of_birth") >= col("year_of_event"))
            | (col("year_of_event") - col("athlete_year_of_birth") > 100)
            | (col("year_of_event") - col("athlete_year_of_birth") < 10),
            None
        ).otherwise(col("athlete_year_of_birth"))
    )

    # Step 7 - Create distance_type
    df = df.withColumn("distance_type",
        when(col("event_distance_or_length").rlike(r"\d+h$"), "time")
        .when(col("event_distance_or_length").rlike(r"^\d+:\d{2}h?$"), "time")
        .when(col("event_distance_or_length").rlike(r"\d+d$"), "days")
        .when(col("event_distance_or_length").rlike(r"(?i)etappen"), "unknown")
        .when(col("event_distance_or_length").rlike(r"(?i)km"), "km")
        .when(col("event_distance_or_length").rlike(r"(?i)mi"), "miles")
        .when(col("event_distance_or_length").isNull(), None)
        .otherwise("unknown")
    )

    # Step 7 - Drop days, unknown and Etappen
    df = (df
        .where(col("distance_type") != "days")
        .where(col("distance_type") != "unknown")
        .where(~col("athlete_performance").rlike(r"^\d+d"))
        .where(~col("event_distance_or_length").rlike(r"(?i)\d+d"))
        .where(~col("event_distance_or_length").rlike(r"(?i)etappen"))
    )

    # Step 7 - Drop invalid performance rows
    df = df.where(
        ~(
            (col("distance_type") == "time") &
            (~col("athlete_performance").rlike(r"^\d+\.?\d* (?i)(km|k|miles|mi|m)$"))
        )
    )

    # Step 7 - performance_seconds and performance_distance
    # Got help from LLM with regex
    df = df.withColumn("performance_seconds",
        when(col("distance_type").isin("km", "miles"),
            regexp_extract(col("athlete_performance"), r"(\d+):(\d{2}):(\d{2})", 1).cast("int") * 3600 +
            regexp_extract(col("athlete_performance"), r"(\d+):(\d{2}):(\d{2})", 2).cast("int") * 60 +
            regexp_extract(col("athlete_performance"), r"(\d+):(\d{2}):(\d{2})", 3).cast("int")
        ).otherwise(None)
    )

    df = df.withColumn("performance_distance",
        when(col("distance_type") == "time",
            regexp_extract(col("athlete_performance"), r"(\d+\.?\d*)", 1).cast("double")
        ).otherwise(None)
    )

    # Step 7 - Calculate average speed
    # Got help from LLM with regex
    df = df.withColumn("average_speed",
        when(col("distance_type").isin("km", "miles"),
            when(col("performance_seconds") > 0,
                round(
                    regexp_extract(col("event_distance_or_length"), r"(\d+\.?\d*)", 1).cast("double") /
                    (col("performance_seconds") / 3600),
                    3
                )
            ).otherwise(None)
        ).when(col("distance_type") == "time",
            when(regexp_extract(col("event_distance_or_length"), r"(\d+\.?\d*)", 1).cast("double") > 0,
                round(
                    col("performance_distance") /
                    regexp_extract(col("event_distance_or_length"), r"(\d+\.?\d*)", 1).cast("double"),
                    3
                )
            ).otherwise(None)
        ).otherwise(None)
    )

    # Step 7 - Drop rows with invalid average speed
    df = df.where(
        (
            (col("distance_type") == "miles") &
            (col("average_speed") <= 11.74)
        ) |
        (
            (col("distance_type") != "miles") &
            (col("average_speed") <= 18.9)
        ) |
        (col("average_speed").isNull())
    )

    # Step 9 - Join country codes
    country_df = spark.table("marathos.bronze.country_codes")
    df = df.join(
        country_df,
        df.athlete_country == country_df.ioc_code,
        "left"
    ).drop(country_df.ioc_code)

    # Step 10 - Fill nulls
    for column in ["athlete_club", "athlete_gender", "athlete_age_category"]:
        df = df.withColumn(column,
            when(col(column).isNull(), "Missing").otherwise(col(column))
        )

    # Step 11 - Giving unique ids to events and athletes(some athlete ids are placeholders)
    df = df.withColumn("event_id",
    sha2(concat_ws("_", col("event_name"), col("year_of_event").cast("string")), 256)
    )

    # Got help from LLM with this
    df = df.withColumn("athlete_id_hash",
    when(col("athlete_id_is_placeholder"),
        sha2(concat_ws("_", 
            col("athlete_id").cast("string"),
            col("event_name"),
            col("year_of_event").cast("string"),
            col("athlete_performance")
        ), 256)
    ).otherwise(sha2(col("athlete_id").cast("string"), 256))
    )

    # Step 12 - Drop original columns
    return df.drop(
        "athlete_performance",
        "athlete_average_speed",
        "athlete_country",
        "event_dates"
    )