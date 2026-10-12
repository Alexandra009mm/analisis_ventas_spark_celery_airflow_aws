from celery_app import app
from config import OUTPUT_PATHS
from io_utils import read_sales, write_parquet
from metrics import build_metrics, show_metrics
from spark_session import create_spark_session
from transform import clean_sales


@app.task(name="etl.read_bronze", queue="bronze")
def read_bronze():
    """Lee el CSV de origen y persiste la capa Bronze."""
    spark = create_spark_session("Sales_ETL_Bronze")
    try:
        bronze = read_sales(spark)
        write_parquet(bronze, OUTPUT_PATHS["bronze"])
        return "Bronze completada"
    finally:
        spark.stop()


@app.task(name="etl.clean_silver", queue="silver")
def clean_silver():
    """Limpia Bronze y persiste las ventas válidas e inválidas."""
    spark = create_spark_session("Sales_ETL_Silver")
    try:
        bronze = spark.read.parquet(str(OUTPUT_PATHS["bronze"]))
        valid_sales, invalid_sales = clean_sales(bronze)
        write_parquet(valid_sales, OUTPUT_PATHS["valid_sales"])
        write_parquet(invalid_sales, OUTPUT_PATHS["invalid_sales"])
        return "Silver completada"
    finally:
        spark.stop()


@app.task(name="etl.build_gold", queue="gold")
def build_gold():
    """Calcula y persiste los indicadores de la capa Gold."""
    spark = create_spark_session("Sales_ETL_Gold")
    try:
        valid_sales = spark.read.parquet(str(OUTPUT_PATHS["valid_sales"]))
        invalid_sales = spark.read.parquet(str(OUTPUT_PATHS["invalid_sales"]))
        metrics = build_metrics(valid_sales, invalid_sales)

        write_parquet(metrics["summary"], OUTPUT_PATHS["summary"])
        write_parquet(
            metrics["revenue_by_product"],
            OUTPUT_PATHS["revenue_by_product"],
        )
        write_parquet(
            metrics["revenue_by_city"],
            OUTPUT_PATHS["revenue_by_city"],
        )
        show_metrics(metrics)
        return "Gold completada"
    finally:
        spark.stop()
