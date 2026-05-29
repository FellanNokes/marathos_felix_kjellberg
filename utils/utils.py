import re

def to_snake_case(name):
    name = name.strip().casefold()
    name = re.sub(r"[/]+", "_or_", name)
    name = re.sub(r"[\s]+", "_", name)
    return name

def rename_columns_to_snake_case(df):
    new_columns = [to_snake_case(column) for column in df.columns]
    return df.toDF(*new_columns)