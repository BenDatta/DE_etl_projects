from datetime import datetime, timedelta
from airflow import DAG  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore
from airflow.providers.postgres.hooks.postgres import PostgresHook  # type: ignore
from huggingface_hub import list_models  # type: ignore

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 8, 12),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="models_etl_huggingface",
    default_args=default_args,
    description="ETL to extract Hugging Face models",
    schedule="@daily",  # ✅ Modern Airflow keyword
    catchup=False,
    tags=["etl", "huggingface", "ai models"],
) as dag:

    def extract_huggingface_models(**kwargs):
        try:
            models = list_models(
                sort="lastModified", direction=-1, limit=50, cardData=True
            )
            model_list = []
            for model in models:
                model_info = {
                    "model_id": model.modelId,
                    "author": model.author,
                    "tags": model.tags,
                    "pipeline_tag": model.pipeline_tag,
                    "last_modified": model.lastModified,
                }
                model_list.append(model_info)
            kwargs["ti"].xcom_push(key="raw_models", value=model_list)
            print(f"Extracted {len(model_list)} models from Hugging Face.")
            return "Extraction successful"

        except Exception as e:
            print(f"Error extracting models: {e}")
            return "Extraction failed"

    extract_task = PythonOperator(
        task_id="extracting_huggingface_models",
        python_callable=extract_huggingface_models,
    )

    def transform_models_data(**kwargs):
        ti = kwargs["ti"]
        raw_models = (
            ti.xcom_pull(task_ids="extracting_huggingface_models", key="raw_models")
            or []
        )
        transformed_models = []
        seen = set()

        for m in raw_models:
            mid = m.get("model_id")
            if not mid or mid in seen:
                continue
            seen.add(mid)

            transformed_models.append(
                {
                    "model_id": mid,
                    "author": m.get("author"),
                    "tags": m.get("tags"),
                    "pipeline_tag": m.get("pipeline_tag"),
                    "last_modified": m.get("last_modified"),
                }
            )

        ti.xcom_push(key="transformed_models", value=transformed_models)
        print(f"Transformed {len(transformed_models)} models.")
        return "Transformation successful"

    transform_task = PythonOperator(
        task_id="transforming_huggingface_models",
        python_callable=transform_models_data,
    )

    def load_to_postgres(**kwargs):
        ti = kwargs["ti"]
        transformed_models = (
            ti.xcom_pull(
                task_ids="transforming_huggingface_models", key="transformed_models"
            )
            or []
        )

        if not transformed_models:
            print("No models to load into Postgres.")
            return "No data to load"

        pg_hook = PostgresHook(postgres_conn_id="models_connection")

        create_table_query = """
        CREATE TABLE IF NOT EXISTS huggingface_models (
            model_id VARCHAR(255) PRIMARY KEY,
            author VARCHAR(255),
            tags TEXT[],
            pipeline_tag VARCHAR(255),
            last_modified TIMESTAMP
        );
        """
        pg_hook.run(create_table_query)

        insert_query = """
        INSERT INTO huggingface_models (model_id, author, tags, pipeline_tag, last_modified)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (model_id) DO UPDATE SET
            author = EXCLUDED.author,
            tags = EXCLUDED.tags,
            pipeline_tag = EXCLUDED.pipeline_tag,
            last_modified = EXCLUDED.last_modified;
        """

        try:
            for model in transformed_models:
                pg_hook.run(
                    insert_query,
                    parameters=(
                        model["model_id"],
                        model["author"],
                        model["tags"],
                        model["pipeline_tag"],
                        model["last_modified"],
                    ),
                )
            print(f"LOAD COMPLETE: Loaded {len(transformed_models)} records")
            return f"Loaded {len(transformed_models)} models"

        except Exception as e:
            print(f"LOAD ERROR: {e}")
            raise

    load_task = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres,
    )

    extract_task >> transform_task >> load_task
