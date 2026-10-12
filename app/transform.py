from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F #F es un alias para el módulo de funciones de PySpark, que proporciona una variedad de funciones para manipular y transformar datos en DataFrames. Se utiliza para acceder a funciones como col, upper, trim, when, concat_ws, etc., de manera más concisa y legible en lugar de escribir pyspark.sql.functions cada vez que se necesite una función específica.


def clean_sales(bronze: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Normaliza la fuente y devuelve ventas válidas e inválidas."""
    city_key = F.translate(
        F.upper(F.trim(F.col("city"))),
        "ÁÉÍÓÚÜÑ",
        "AEIOUUN", #Que es esto? es una función que reemplaza los caracteres acentuados y la ñ por sus equivalentes sin acento o sin tilde. Esto se hace para normalizar los nombres de las ciudades y evitar problemas de comparación o agrupamiento debido a diferencias en la escritura de los nombres.
    )
    product_key = F.upper(F.trim(F.col("product")))

    parsed = ( # por qué se llama parsed? porque es el resultado de analizar y transformar los datos de ventas en un formato más limpio y estructurado, listo para su posterior procesamiento y análisis. El nombre "parsed" indica que los datos han sido procesados y normalizados a partir de la fuente original (bronze).
        bronze
        .withColumn("order_id", F.expr("try_cast(trim(order_id) AS BIGINT)"))
        .withColumn(
            "order_date",
            F.to_date(
                F.try_to_timestamp(
                    F.trim(F.col("order_date")),
                    F.lit("yyyy-MM-dd"),
                )
            ),
        )
        .withColumn(
            "customer_id",
            F.when(
                F.length(F.trim(F.col("customer_id"))) > 0, # aquí se está verificando si la longitud del valor de la columna "customer_id" después de eliminar los espacios en blanco es mayor que 0. Si es así, significa que hay un valor válido en esa columna y se mantiene. Si no, se asigna un valor nulo (None) a esa columna.
                F.trim(F.col("customer_id")),
            ),
        )
        .withColumn( #el withColumn se utiliza para crear o reemplazar una columna en un DataFrame. En este caso, se está creando una nueva columna llamada "city" que contiene los nombres de las ciudades normalizados a partir de la columna "city_key". Se utiliza la función F.when para asignar un valor específico a la columna "city" según el valor de "city_key". Si "city_key" coincide con uno de los valores especificados (por ejemplo, "BARRANQUILLA"), se asigna el nombre correspondiente (por ejemplo, "Barranquilla"). Si no hay coincidencia, la columna "city" será nula.
            "city",
            F.when(city_key == "BARRANQUILLA", "Barranquilla")
            .when(city_key == "BOGOTA", "Bogotá")
            .when(city_key == "MEDELLIN", "Medellín")
            .when(city_key == "CALI", "Cali")
            .when(city_key == "CARTAGENA", "Cartagena")
            .when(city_key == "SANTA MARTA", "Santa Marta"),
        )
        .withColumn(
            "product",
            F.when(product_key == "TECLADO", "Teclado")
            .when(product_key == "MOUSE", "Mouse")
            .when(product_key == "MONITOR", "Monitor")
            .when(product_key == "AUDIFONOS", "Audifonos")
            .when(product_key == "WEBCAM", "Webcam"),
        )
        .withColumn("quantity", F.expr("try_cast(trim(quantity) AS INT)"))# que es una expresion try_cast y por que hay una? es una función que intenta convertir un valor a un tipo de datos específico (en este caso, INT) y devuelve nulo si la conversión falla. Se utiliza para manejar posibles errores de conversión en los datos de ventas, como valores no numéricos o vacíos en la columna "quantity". 
        .withColumn(
            "price",
            F.expr("try_cast(trim(price) AS DECIMAL(18, 2))"),#ejemplo del try_cast: si la columna "price" contiene un valor como "abc" o "", la función try_cast devolverá nulo en lugar de generar un error. Esto permite que el proceso de limpieza continúe sin interrupciones y que las filas con valores inválidos se identifiquen y manejen adecuadamente más adelante en el flujo de trabajo.
        )
    )

    # Conserva una fila por order_id; las apariciones siguientes son inválidas.
    #como asi? esto significa que si hay varias filas con el mismo order_id, solo se considerará válida la primera aparición de ese order_id y las demás se marcarán como inválidas. Esto se hace para evitar duplicados en los datos de ventas y garantizar que cada order_id sea único en el conjunto de datos válido.    
    duplicate_window = Window.partitionBy("order_id").orderBy(
        "order_date",
        "customer_id",
        "city",
        "product",
        "quantity",
        "price",
    )
    checked = (
        parsed
        .withColumn("_row_number", F.row_number().over(duplicate_window))
        .withColumn(
            "is_duplicate",
            F.col("order_id").isNotNull() & (F.col("_row_number") > 1),
        )
        .drop("_row_number")
    )

    # Acumula en una sola columna los motivos de invalidez de cada fila.
    invalid_reason = F.concat_ws(
        "; ",
        F.when(F.col("order_id").isNull(), "invalid_order_id"),
        F.when(F.col("order_date").isNull(), "invalid_order_date"),
        F.when(F.col("customer_id").isNull(), "missing_customer_id"),
        F.when(F.col("city").isNull(), "invalid_city"),
        F.when(F.col("product").isNull(), "invalid_product"),
        F.when(
            F.col("quantity").isNull() | (F.col("quantity") <= 0),
            "invalid_quantity",
        ),
        F.when(
            F.col("price").isNull() | (F.col("price") <= 0),
            "invalid_price",
        ),
        F.when(F.col("is_duplicate"), "duplicate_order_id"),
    )
    checked = checked.withColumn("_invalid_reason", invalid_reason)

    valid_condition = F.length(F.col("_invalid_reason")) == 0
    valid_sales = (
        checked
        .filter(valid_condition)
        .drop("is_duplicate", "_invalid_reason")
        .withColumn(
            "revenue",
            (F.col("quantity") * F.col("price")).cast("DECIMAL(20, 2)"),
        )
    )
    invalid_sales = (
        checked
        .filter(~valid_condition)
        .withColumnRenamed("_invalid_reason", "invalid_reason")
        .drop("is_duplicate")
    )
    return valid_sales, invalid_sales
