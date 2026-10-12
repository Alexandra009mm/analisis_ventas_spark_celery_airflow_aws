import os

from celery import Celery


app = Celery(
    "sales_etl",
    broker=os.environ["CELERY_BROKER_URL"],
    backend=os.environ.get("CELERY_RESULT_BACKEND", "rpc://"),
    include=["tasks"],
)

app.conf.update(
    task_routes={
        "etl.read_bronze": {"queue": "bronze"},
        "etl.clean_silver": {"queue": "silver"},
        "etl.build_gold": {"queue": "gold"},
    },
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    task_track_started=True,
)
