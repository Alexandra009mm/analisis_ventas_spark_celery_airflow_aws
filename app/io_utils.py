from pathlib import Path

from pyspark.sql import SparkSession, DataFrame

from config import SALES_SCHEMA, SOURCE_PATH


def read_sales(spark: SparkSession) -> DataFrame:
    """Lee la fuente como texto para detectar valores mal formados."""
    SOURCE_PATH.parent.mkdir(parents=True, exist_ok=True)
    return (
        spark.read
        .option("header", True)
        .schema(SALES_SCHEMA)
        .csv(str(SOURCE_PATH))
    )


def write_parquet(dataframe: DataFrame, path):
    """Crea la carpeta de destino y guarda el DataFrame en formato Parquet."""
    output_path = Path(path)
    output_path.mkdir(parents=True, exist_ok=True)
    dataframe.write.mode("overwrite").parquet(str(output_path))
