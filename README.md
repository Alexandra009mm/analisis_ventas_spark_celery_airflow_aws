# ETL de ventas: Spark + Celery

Pipeline local que lee ventas desde CSV, las procesa con PySpark y guarda los
resultados en formato Parquet. Celery distribuye las etapas entre workers y
RabbitMQ funciona como broker.

> Este proyecto se ejecuta localmente con Docker Compose. No está configurado
> para AWS, S3, ECS ni ECR.

## Flujo de datos

```text
data/raw/sales.csv
       |
       v
Bronze: copia de los datos de entrada
       |
       v
Silver: ventas válidas + ventas rechazadas
       |
       v
Gold: indicadores y resúmenes
```

Celery ejecuta las tareas en orden:

```text
read_bronze -> clean_silver -> build_gold
```

## Requisitos

- Docker Desktop con Docker Compose.

Python, Java, Spark y Celery se ejecutan dentro de la imagen Docker; no hace
falta instalarlos en Windows.

## Preparar el archivo de entrada

El CSV debe incluir estas columnas:

```text
order_id,order_date,customer_id,city,product,quantity,price
```

El dispatcher ejecuta automáticamente el generador incluido en la imagen.
También puedes generar el archivo manualmente desde la raíz del proyecto:

```powershell
python data/send.py
```

El generador reemplaza `data/raw/sales.csv` y crea cinco millones de ventas más
duplicados. Escribe las filas progresivamente para no guardar todo el conjunto
en memoria.

## Ejecutar el ETL

Abre PowerShell en la raíz del proyecto.

### 1. Construir la imagen

```powershell
docker compose build dispatcher
```

La imagen incluye el código de `app/` y se etiqueta como
`spark-exe-etl:local`.

### 2. Iniciar RabbitMQ y los workers

```powershell
docker compose up -d rabbitmq worker-bronze worker-silver worker-gold
```

Espera a que RabbitMQ esté saludable y los workers estén activos:

```powershell
docker compose ps
```

### 3. Generar los datos y encolar una ejecución

```powershell
docker compose run --rm --no-deps dispatcher
```

Compose monta `./data` desde el host en `/app/data` (y crea la carpeta local si
falta). El contenedor `dispatcher` ejecuta primero el generador que viene
incluido en la imagen; este crea `data/raw/` y reemplaza `sales.csv`. Luego
encola la cadena Celery y termina. Las tareas crean las carpetas de salida
Bronze, Silver y Gold al guardar los Parquet.

También puedes iniciar todo el proceso en un solo paso:

```powershell
docker compose up --build -d
```

Si quieres repetirlo con nuevos datos después de que el dispatcher haya
terminado, vuelve a ejecutarlo:

```powershell
docker compose up -d --force-recreate dispatcher
```

**Atención:** el dispatcher genera un CSV nuevo cada vez que se ejecuta. Si
quieres procesar un CSV propio, no uses el comando configurado del dispatcher
sin quitar primero la llamada a `data/send.py`.

Para seguirlo:

```powershell
docker compose logs -f worker-bronze worker-silver worker-gold
```

Busca los mensajes `succeeded` de `etl.read_bronze`, `etl.clean_silver` y
`etl.build_gold` para confirmar que terminó cada etapa. Que el dispatcher
termine correctamente solo confirma que encoló la cadena, no que el ETL ya
haya concluido.

## Consultar los resultados

Todos los resultados están bajo `data/` en tu máquina:

| Capa | Ubicación | Contenido |
| --- | --- | --- |
| Bronze | `data/bronze/sales/` | Copia Parquet de la entrada |
| Silver | `data/silver/valid_sales/` | Ventas válidas, incluida la columna `revenue` |
| Silver | `data/silver/invalid_sales/` | Filas rechazadas y su columna `invalid_reason` |
| Gold | `data/gold/sales_summary/` | Ingreso, unidades y conteos de transacciones |
| Gold | `data/gold/revenue_by_product/` | Unidades e ingresos por producto |
| Gold | `data/gold/revenue_by_city/` | Ingresos por ciudad |

Spark escribe archivos `part-*.parquet` dentro de cada carpeta. El archivo
vacío `_SUCCESS` indica que la escritura terminó; no contiene los resultados.
Gold también imprime los resúmenes en los logs de `worker-gold`:

```powershell
docker compose logs worker-gold
```

## Reglas de validación

Silver normaliza espacios, mayúsculas y variantes de acentos en ciudades
conocidas, y estandariza los nombres de productos reconocidos.

Una fila se considera inválida si tiene:

- Un `order_id` que no se puede convertir o que está duplicado.
- Una fecha que no cumple el formato `yyyy-MM-dd`.
- Un `customer_id` vacío.
- Una ciudad o un producto no reconocido.
- Una cantidad no numérica o menor/igual a cero.
- Un precio no numérico o menor/igual a cero.

Una fila puede tener varios motivos. Para cada `order_id` repetido se conserva
una fila y las demás se marcan como duplicadas.

## Volver a ejecutar y detener

Para procesar otro conjunto de datos, reemplaza `data/raw/sales.csv` y vuelve
a encolar el dispatcher. Las tareas escriben con `overwrite`, por lo que cada
ejecución reemplaza las salidas Bronze, Silver y Gold existentes.

No ejecutes dos cadenas al mismo tiempo sobre las mismas rutas: podrían
sobrescribir las salidas una de otra.

Detén RabbitMQ y los workers con:

```powershell
docker compose down
```

Esto conserva `data/` y el volumen de RabbitMQ. No agregues `-v` si quieres
conservar ese volumen.

## Alcance actual

- RabbitMQ, Celery y Spark corren en contenedores Docker locales.
- Cada tarea inicia Spark con `local[*]`; Spark procesa dentro del contenedor
  del worker y no forma un clúster distribuido.
- El CSV y los resultados permanecen en el sistema de archivos local.
- Los workers usan la misma imagen del proyecto y consumen las colas `bronze`,
  `silver` y `gold`, respectivamente.
- No hay pruebas automatizadas configuradas actualmente.
