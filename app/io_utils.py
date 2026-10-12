from pyspark.sql import SparkSession, DataFrame

from config import SALES_SCHEMA, SOURCE_PATH


def read_sales(spark: SparkSession) -> DataFrame:
    """Lee la fuente como texto para detectar valores mal formados."""
    return (
        spark.read
        .option("header", True)
        .schema(SALES_SCHEMA)
        .csv(str(SOURCE_PATH))
    )


def write_parquet(dataframe: DataFrame, path):
    """Guarda un DataFrame en formato Parquet."""
    dataframe.write.mode("overwrite").parquet(str(path))
