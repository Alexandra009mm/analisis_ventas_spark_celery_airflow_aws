from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# por que se llama build_metrics? porque esta función se encarga de construir o generar las métricas a partir de los datos de ventas válidos e inválidos. El término "build" (construir) indica que la función toma los datos de entrada y realiza cálculos y agregaciones para producir un conjunto de métricas que resumen el rendimiento y la calidad de los datos de ventas.

def build_metrics(
    valid_sales: DataFrame,
    invalid_sales: DataFrame,
) -> dict[str, DataFrame]:
    """Crea las tablas Gold con resumen e ingresos agrupados."""
    valid_count = valid_sales.agg(
        F.count(F.lit(1)).alias("valid_transactions")
    )
    invalid_count = invalid_sales.agg(
        F.count(F.lit(1)).alias("invalid_transactions")
    )
    quality = (
        valid_count
        .crossJoin(invalid_count)
        .withColumn(
            "total_transactions",
            F.col("valid_transactions") + F.col("invalid_transactions"),
        )
        .select(
            "total_transactions",
            "valid_transactions",
            "invalid_transactions",
        )
    )

    summary = (
        valid_sales.agg(
            F.coalesce(
                F.sum("revenue"), F.lit(0).cast("DECIMAL(20, 2)")
            ).alias("total_revenue"),
            F.coalesce(F.sum("quantity"), F.lit(0)).alias("units_sold"),
        )
        .crossJoin(quality)
    )
    revenue_by_product = (
        valid_sales.groupBy("product")
        .agg(
            F.sum("quantity").alias("units_sold"),
            F.sum("revenue").alias("total_revenue"),
        )
        .orderBy(F.desc("total_revenue"), F.asc("product"))
    )
    revenue_by_city = (
        valid_sales.groupBy("city")
        .agg(F.sum("revenue").alias("total_revenue"))
        .orderBy(F.desc("total_revenue"), F.asc("city"))
    )
    invalid_reasons = (
        invalid_sales.groupBy("invalid_reason")
        .count()
        .orderBy(F.desc("count"))
    )

    return {
        "summary": summary,
        "revenue_by_product": revenue_by_product,
        "revenue_by_city": revenue_by_city,
        "invalid_reasons": invalid_reasons,
    }

# por que se llama show_metrics? porque esta función se encarga de mostrar o imprimir las métricas generadas en un formato legible para el usuario. El término "show" (mostrar) indica que la función toma las métricas calculadas y las presenta en la consola o en un formato visual, permitiendo al usuario revisar y analizar los resultados de manera clara y comprensible.

#De donde viene el metrics? metrics es un diccionario que contiene los DataFrames resultantes de la función build_metrics. Cada clave del diccionario representa un tipo específico de métrica (por ejemplo, "summary", "revenue_by_product", "revenue_by_city", "invalid_reasons"), y el valor asociado a cada clave es un DataFrame que contiene los datos correspondientes a esa métrica. La función show_metrics utiliza este diccionario para acceder a los DataFrames y mostrarlos en la consola.
def show_metrics(metrics: dict[str, DataFrame]):
    """Imprime las respuestas y los motivos de las filas inválidas."""
    print("Resumen de ventas válidas:")
    metrics["summary"].show(truncate=False)
    print("Resumen por producto (ordenado por ingreso):")
    metrics["revenue_by_product"].show(truncate=False)
    print("Producto con mayor ingreso:")
    metrics["revenue_by_product"].limit(1).show(truncate=False)
    print("Resumen por ciudad (ordenado por ingreso):")
    metrics["revenue_by_city"].show(truncate=False)
    print("Ciudad con mayor ingreso:")
    metrics["revenue_by_city"].limit(1).show(truncate=False)
    print("Transacciones inválidas (con motivo):")
    metrics["invalid_reasons"].show(truncate=False)
