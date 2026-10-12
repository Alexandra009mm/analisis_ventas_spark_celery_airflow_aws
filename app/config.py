from pathlib import Path


# Al cambiar de almacenamiento, centraliza aquí la fuente y los destinos.
PROJECT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_DIR / "data"
SOURCE_PATH = DATA_DIR / "raw" / "sales.csv"

OUTPUT_PATHS = {
    "bronze": DATA_DIR / "bronze" / "sales",
    "valid_sales": DATA_DIR / "silver" / "valid_sales",
    "invalid_sales": DATA_DIR / "silver" / "invalid_sales",
    "summary": DATA_DIR / "gold" / "sales_summary",
    "revenue_by_product": DATA_DIR / "gold" / "revenue_by_product",
    "revenue_by_city": DATA_DIR / "gold" / "revenue_by_city",
}

SALES_SCHEMA = (
    "order_id STRING, order_date STRING, customer_id STRING, "
    "city STRING, product STRING, quantity STRING, price STRING"
)
