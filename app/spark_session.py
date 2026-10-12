from pyspark.sql import SparkSession


def create_spark_session(app_name: str) -> SparkSession:
    """Crea una sesión Spark local para una etapa del ETL."""
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .getOrCreate()
    )
