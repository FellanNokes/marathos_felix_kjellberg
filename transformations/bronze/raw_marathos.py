from pyspark import pipelines as dp

BASE_DIR = "/Volumes/marathos/default/raw"

schema = (
    spark.read.format("csv")
    .options(header=True, inferSchema=True)
    .load(f"{BASE_DIR}/data/TWO_CENTURIES_OF_UM_RACES.csv")
    .schema
)

@dp.table(
    name="marathos.bronze.raw_marathos",
    comment="Raw marathos data",
    table_properties={
        "delta.columnMapping.mode": "name",
        "delta.minReaderVersion": "2",
        "delta.minWriterVersion": "5",
    },
)
def raw_marathos():
    return(
        spark.readStream.format("csv")
        .options(header=True, encoding="UTF-8")
        .schema(schema)
        .load(f"{BASE_DIR}/data")
    )