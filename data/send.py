
import csv
import os
import random
from datetime import date, timedelta
from pathlib import Path

# Configuración
random.seed(42)
TOTAL_REGISTROS = int(os.environ.get("TOTAL_REGISTROS", "5000000"))
ARCHIVO_SALIDA = Path(__file__).resolve().parents[1] / "data" / "raw" / "sales.csv"

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

cantidad_duplicados = int(TOTAL_REGISTROS * 0.02)
indices_duplicados = set(
    random.Random(43).sample(range(TOTAL_REGISTROS), cantidad_duplicados)
)

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

ARCHIVO_SALIDA.parent.mkdir(parents=True, exist_ok=True)
with open(
    ARCHIVO_SALIDA,
    mode="w",
    newline="",
    encoding="utf-8"
) as archivo:

    writer = csv.writer(archivo)
    writer.writerow(columnas)

    # Genera y escribe cada fila inmediatamente para limitar el uso de memoria.
    for i in range(TOTAL_REGISTROS):
        order_id = 1001 + i
        fecha = fecha_inicio + timedelta(days=random.randint(0, 45))
        customer_id = f"C{random.randint(1, 900):03d}"
        city = random.choice(ciudades)
        product = random.choice(list(productos.keys()))
        quantity = random.randint(1, 8)
        price = productos[product]
        order_date = fecha.isoformat()

        # Inserta errores de calidad para probar las validaciones del ETL.
        if random.random() < 0.05:
            quantity = random.choice([0, -1, -2])
        if random.random() < 0.01:
            quantity = random.choice(["tres", "dos", ""])
        if random.random() < 0.03:
            price = random.choice([-price, "", "ochenta mil"])
        if random.random() < 0.02:
            customer_id = ""
        if random.random() < 0.05:
            city = random.choice(
                [" barranquilla ", "BOGOTA", "medellín", "", "Bogotá"]
            )
        if random.random() < 0.03:
            product = random.choice(["teclado", " Mouse ", "AUDIFONOS", ""])
        if random.random() < 0.01:
            order_date = random.choice(
                ["2026-13-40", "2026/09/12", "fecha_invalida"]
            )

        registro = [
            order_id,
            order_date,
            customer_id,
            city,
            product,
            quantity,
            price,
        ]
        writer.writerow(registro)

        # Duplica un 2% de las ventas base para probar el control de duplicados.
        if i in indices_duplicados:
            writer.writerow(registro)

print("Datos generados correctamente")
print(f"Archivo: {ARCHIVO_SALIDA}")
print(f"Registros generados: {TOTAL_REGISTROS + cantidad_duplicados}")
print(f"Duplicados agregados: {cantidad_duplicados}")
