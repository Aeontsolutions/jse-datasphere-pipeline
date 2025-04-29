from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
import pandas as pd
from airflow.datasets import Dataset

PROJECT_ID = "jse-datasphere"
DATASET_ID = "jse_seeds"
TABLE_ID = "financial_documents"

# Define the BigQuery dataset
bq_dataset = Dataset("bq://jse-datasphere/jse_seeds/financial_documents")


def read_from_bigquery(**context):
    bq_hook = BigQueryHook(gcp_conn_id='google_cloud_default')
    client = bq_hook.get_client()
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    query = f"SELECT * FROM `{table_ref}`"
    df = bq_hook.get_pandas_df(sql=query, dialect='standard')
    row_count = len(df)
    context['ti'].log.info(f"Read {row_count} rows from {table_ref}")
    context['ti'].log.info(f"Sample data:\n{df.head()}")
    return f"Read {row_count} rows from BigQuery."


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'bigquery_downstream_dag',
    default_args=default_args,
    description='Reads from BigQuery after upstream DAG completes',
    schedule=[bq_dataset],  # Dataset-aware scheduling
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    read_task = PythonOperator(
        task_id='read_from_bigquery',
        python_callable=read_from_bigquery,
    ) 