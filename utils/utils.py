import re
from pyspark.sql.functions import (
    col,
    when,
    to_date,
    regexp_extract,
    concat,
    lit,
    round,
    sha2,
    concat_ws,
    monotonically_increasing_id,
)


def to_snake_case(name):
    name = name.strip().casefold()
    name = re.sub(r"[/]+", "_or_", name)
    name = re.sub(r"[\s]+", "_", name)
    return name


def rename_columns_to_snake_case(df):
    """
    changes columns name to snake_case from df
    """
    new_columns = [to_snake_case(column) for column in df.columns]
    return df.toDF(*new_columns)


def parse_event_dates(df):
    """
    Parses event_dates column into start_date and end_date.
    Handles three formats:
    - Single date: dd.MM.yyyy
    - Same month span: dd.-dd.MM.yyyy
    - Cross month/year span: dd.MM.yyyy-dd.MM.yyyy
    Got help from LLM with regex patterns.
    """
    cross_month_pattern = r"\d{2}\.\d{2}\.\d{4}-\d{2}\.\d{2}\.\d{4}"
    same_month_pattern = r"^\d{2}\.-\d{2}\.\d{2}\.\d{4}"

    return df.withColumn(
        "start_date",
        when(
            col("event_dates").rlike(cross_month_pattern),
            to_date(
                regexp_extract(col("event_dates"), r"^(\d{2}\.\d{2}\.\d{4})", 1),
                "dd.MM.yyyy",
            ),
        )
        .when(
            col("event_dates").rlike(same_month_pattern),
            to_date(
                concat(
                    regexp_extract(col("event_dates"), r"^(\d{2})\.", 1),
                    lit("."),
                    regexp_extract(col("event_dates"), r"-\d{2}\.(\d{2}\.\d{4})$", 1),
                ),
                "dd.MM.yyyy",
            ),
        )
        .otherwise(to_date(col("event_dates"), "dd.MM.yyyy")),
    ).withColumn(
        "end_date",
        when(
            col("event_dates").rlike(cross_month_pattern),
            to_date(
                regexp_extract(col("event_dates"), r"(\d{2}\.\d{2}\.\d{4})$", 1),
                "dd.MM.yyyy",
            ),
        )
        .when(
            col("event_dates").rlike(same_month_pattern),
            to_date(
                concat(
                    regexp_extract(col("event_dates"), r"-(\d{2})\.\d{2}\.\d{4}$", 1),
                    lit("."),
                    regexp_extract(col("event_dates"), r"-\d{2}\.(\d{2}\.\d{4})$", 1),
                ),
                "dd.MM.yyyy",
            ),
        )
        .otherwise(None),
    )


def convert_none_strings_to_null(df):
    """Converts string 'None' values to actual null across all string columns"""
    string_cols = [c for c, t in df.dtypes if t == "string"]
    for column in string_cols:
        df = df.withColumn(
            column, when(col(column) == "None", None).otherwise(col(column))
        )
    return df


def fill_missing_string_cols(df, columns):
    """Fills null values with 'Missing' for specified string columns"""
    for column in columns:
        df = df.withColumn(
            column, when(col(column).isNull(), "Missing").otherwise(col(column))
        )
    return df


def create_sha2_ids(df):
    """
    Creates unique hash IDs for events, athletes and results.
    Placeholder athletes get unique ID per race to avoid ID collision.
    Got help from LLM with this.
    """
    df = df.withColumn(
        "event_id",
        sha2(
            concat_ws("_", col("event_name"), col("year_of_event").cast("string")), 256
        ),
    )
    df = df.withColumn(
        "athlete_id_hash",
        when(
            col("athlete_id_is_placeholder"),
            sha2(
                concat_ws(
                    "_",
                    col("athlete_id").cast("string"),
                    col("event_name"),
                    col("year_of_event").cast("string"),
                    col("athlete_performance"),
                ),
                256,
            ),
        ).otherwise(sha2(col("athlete_id").cast("string"), 256)),
    )
    df = df.withColumn(
        "result_id", sha2(concat_ws("_", col("athlete_id_hash"), col("event_id")), 256)
    )
    return df

def process_performance(df):
    """
    Creates distance_type, drops invalid rows and calculates
    performance_seconds, performance_distance and average_speed.
    Got help from LLM with regex patterns.
    """
    # Create distance_type
    df = df.withColumn(
        "distance_type",
        when(col("event_distance_or_length").rlike(r"\d+h$"), "time")
        .when(col("event_distance_or_length").rlike(r"^\d+:\d{2}h?$"), "time")
        .when(col("event_distance_or_length").rlike(r"\d+d$"), "days")
        .when(col("event_distance_or_length").rlike(r"(?i)etappen"), "unknown")
        .when(col("event_distance_or_length").rlike(r"(?i)km"), "km")
        .when(col("event_distance_or_length").rlike(r"(?i)mi"), "miles")
        .when(col("event_distance_or_length").rlike(r"^\d+\.?\d*[kK]$"), "km")
        .when(col("event_distance_or_length").rlike(r"^\d+\.?\d*[mM]$"), "miles")
        .when(col("event_distance_or_length").isNull(), None)
        .otherwise("unknown")
    )

    # Drop invalid rows
    df = (df
        .where(col("distance_type") != "days")
        .where(col("distance_type") != "unknown")
        .where(~col("athlete_performance").rlike(r"^\d+d"))
        .where(~col("event_distance_or_length").rlike(r"(?i)\d+d"))
        .where(~col("event_distance_or_length").rlike(r"(?i)etappen"))
        .where(~(
            (col("distance_type") == "time") &
            (~col("athlete_performance").rlike(r"^\d+\.?\d* (?i)(km|k|miles|mi|m)$"))
        ))
    )

    # performance_seconds for km/miles races
    df = df.withColumn(
        "performance_seconds",
        when(col("distance_type").isin("km", "miles"),
            regexp_extract(col("athlete_performance"), r"(\d+):(\d{2}):(\d{2})", 1).cast("int") * 3600 +
            regexp_extract(col("athlete_performance"), r"(\d+):(\d{2}):(\d{2})", 2).cast("int") * 60 +
            regexp_extract(col("athlete_performance"), r"(\d+):(\d{2}):(\d{2})", 3).cast("int")
        ).otherwise(None)
    )

    # performance_distance for time races
    df = df.withColumn(
        "performance_distance",
        when(col("distance_type") == "time",
            regexp_extract(col("athlete_performance"), r"(\d+\.?\d*)", 1).cast("double")
        ).otherwise(None)
    )

    # average_speed
    df = df.withColumn(
        "average_speed",
        when(col("distance_type").isin("km", "miles"),
            when(col("performance_seconds") > 0,
                round(
                    regexp_extract(col("event_distance_or_length"), r"(\d+\.?\d*)", 1).cast("double") /
                    (col("performance_seconds") / 3600), 3
                )
            ).otherwise(None)
        ).when(col("distance_type") == "time",
            when(regexp_extract(col("event_distance_or_length"), r"(\d+\.?\d*)", 1).cast("double") > 0,
                round(
                    col("performance_distance") /
                    regexp_extract(col("event_distance_or_length"), r"(\d+\.?\d*)", 1).cast("double"), 3
                )
            ).otherwise(None)
        ).otherwise(None)
    )

    # Drop invalid average speed
    return df.where(
        ((col("distance_type") == "miles") & (col("average_speed") <= 11.74)) |
        ((col("distance_type") != "miles") & (col("average_speed") <= 18.9)) |
        (col("average_speed").isNull())
    )


def process_event_distance(df):
    """
    Extracts event_distance_km for km/miles races and
    event_duration_hours for time races.
    Got help from LLM with regex patterns.
    """
    # event_distance_km
    df = df.withColumn(
        "event_distance_km",
        when(col("distance_type") == "km",
            regexp_extract(col("event_distance_or_length"), r"(\d+\.?\d*)", 1).cast("double")
        ).when(col("distance_type") == "miles",
            round(
                regexp_extract(col("event_distance_or_length"), r"(\d+\.?\d*)", 1).cast("double") * 1.60934, 2
            )
        ).otherwise(None)
    )

    # event_duration_hours
    return df.withColumn(
        "event_duration_hours",
        when(col("distance_type") == "time",
            when(col("event_distance_or_length").rlike(r"^\d+:\d{2}h?$"),
                round(
                    regexp_extract(col("event_distance_or_length"), r"^(\d+):", 1).cast("double") +
                    regexp_extract(col("event_distance_or_length"), r":(\d{2})h?$", 1).cast("double") / 60, 2
                )
            ).otherwise(
                round(
                    regexp_extract(col("event_distance_or_length"), r"(\d+\.?\d*)", 1).cast("double"), 2
                )
            )
        ).otherwise(None)
    )
