
import csv
import random
from datetime import date, timedelta

# Configuración
random.seed(42)
TOTAL_REGISTROS = 5000000
ARCHIVO_SALIDA = "data/raw/sales.csv"

ciudades = [
    "Barranquilla",
    "Bogota",
    "Medellin",
    "Cali",
    "Cartagena",
    "Santa Marta"
]

productos = {
    "Teclado": 85000,
    "Mouse": 45000,
    "Monitor": 780000,
    "Audifonos": 120000,
    "Webcam": 95000
}

fecha_inicio = date(2026, 9, 1)

registros = []

for i in range(TOTAL_REGISTROS):
    order_id = 1001 + i

    fecha = fecha_inicio + timedelta(
        days=random.randint(0, 45)
    )

    customer_id = f"C{random.randint(1, 900):03d}"
    city = random.choice(ciudades)
    product = random.choice(list(productos.keys()))
    quantity = random.randint(1, 8)
    price = productos[product]

    order_date = fecha.isoformat()

    # 1. Cantidades incorrectas
    if random.random() < 0.05:
        quantity = random.choice([0, -1, -2])

    # 2. Cantidades que no son números
    if random.random() < 0.01:
        quantity = random.choice(["tres", "dos", ""])

    # 3. Precios incorrectos
    if random.random() < 0.03:
        price = random.choice([
            -price,
            "",
            "ochenta mil"
        ])

    # 4. Clientes nulos
    if random.random() < 0.02:
        customer_id = ""

    # 5. Ciudades inconsistentes
    if random.random() < 0.05:
        city = random.choice([
            " barranquilla ",
            "BOGOTA",
            "medellín",
            "",
            "Bogotá"
        ])

    # 6. Productos inconsistentes
    if random.random() < 0.03:
        product = random.choice([
            "teclado",
            " Mouse ",
            "AUDIFONOS",
            ""
        ])

    # 7. Fechas inválidas
    if random.random() < 0.01:
        order_date = random.choice([
            "2026-13-40",
            "2026/09/12",
            "fecha_invalida"
        ])

    registro = [
        order_id,
        order_date,
        customer_id,
        city,
        product,
        quantity,
        price
    ]

    registros.append(registro)

# 8. Duplicar aproximadamente el 2% de los registros
cantidad_duplicados = int(TOTAL_REGISTROS * 0.02)

for _ in range(cantidad_duplicados):
    indice = random.randrange(len(registros))
    registros.append(registros[indice].copy())

# Mezclar para que los duplicados no queden todos al final
random.shuffle(registros)

# Guardar CSV
columnas = [
    "order_id",
    "order_date",
    "customer_id",
    "city",
    "product",
    "quantity",
    "price"
]

with open(
    ARCHIVO_SALIDA,
    mode="w",
    newline="",
    encoding="utf-8"
) as archivo:

    writer = csv.writer(archivo)
    writer.writerow(columnas)
    writer.writerows(registros)

print("Datos generados correctamente")
print(f"Archivo: {ARCHIVO_SALIDA}")
print(f"Registros generados: {len(registros)}")
print(f"Duplicados agregados: {cantidad_duplicados}")
