from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
import pandas as pd
from airflow.datasets import Dataset
from airflow.utils.log.logging_mixin import LoggingMixin
PROJECT_ID = "jse-datasphere"
DATASET_ID = "jse_seeds"
TABLE_ID = "financial_documents"

# Define the BigQuery dataset
bq_dataset = Dataset("bq://jse-datasphere/jse_seeds/financial_documents")

@task
def read_from_bigquery(**context):
    logger = LoggingMixin().log
    bq_hook = BigQueryHook(gcp_conn_id='google_cloud_default')
    client = bq_hook.get_client()
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    # Use Airflow macros for interval
    start = context['data_interval_start']
    end = context['data_interval_end']
    logger.info(f"Reading rows from {start} to {end}")
    query = f'''
        SELECT * FROM `{table_ref}`
        WHERE loaded_at >= TIMESTAMP('{start}') AND loaded_at < TIMESTAMP('{end}')
    '''
    df = bq_hook.get_pandas_df(sql=query, dialect='standard')
    row_count = len(df)
    context['ti'].log.info(f"Read {row_count} new rows from {table_ref}")
    context['ti'].log.info(f"Sample data:\n{df.head()}")
    return f"Read {row_count} new rows from BigQuery."

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
    
    read_task = read_from_bigquery() 
    
    read_task