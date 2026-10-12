from celery import chain

from tasks import build_gold, clean_silver, read_bronze


def main():
    """Encola las etapas en orden; Celery las entrega a sus workers."""
    workflow = chain(
        read_bronze.si(),
        clean_silver.si(),
        build_gold.si(),
    )
    result = workflow.apply_async()
    print(f"ETL encolado en Celery. ID de la tarea final: {result.id}")


if __name__ == "__main__":
    main()
