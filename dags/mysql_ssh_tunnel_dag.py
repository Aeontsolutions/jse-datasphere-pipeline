from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from sshtunnel import SSHTunnelForwarder
import pymysql
import pandas as pd
import os
from google.cloud import bigquery
from airflow.datasets import Dataset
from airflow.exceptions import AirflowSkipException

# BigQuery configuration
PROJECT_ID = "jse-datasphere"  # Replace with your GCP project ID
DATASET_ID = "jse_seeds"
TABLE_ID = "financial_documents"

# Define the BigQuery asset
bq_dataset = Dataset("bq://jse-datasphere/jse_seeds/financial_documents")

@task
def get_connection_configs():
    # Get MySQL connection details
    mysql_conn = BaseHook.get_connection('jse_mysql')
    
    # Get SSH connection details
    ssh_conn = BaseHook.get_connection('jse_ssh_tunnel')
    
    ssh_config = {
        'ssh_host': ssh_conn.host,
        'ssh_username': ssh_conn.login,
        'ssh_private_key': '/opt/airflow/ssh_keys/id_rsa',
        'ssh_port': ssh_conn.port or 22,
    }
    
    mysql_config = {
        'mysql_host': mysql_conn.host or 'localhost',
        'mysql_port': mysql_conn.port or 3306,
        'mysql_user': mysql_conn.login,
        'mysql_password': mysql_conn.password,
        'mysql_database': 'jsewebsite',
    }
    
    return ssh_config, mysql_config

# SQL query
QUERY = """
with 
    inst as (select
        i.InstrumentID
        ,it.term_id
        ,it.slug
        ,i.InstrumentCode
        ,i.InstrumentName
        ,irel.*
    from 
        portal_repo_Instrument i
        join jseweb_terms it on it.name = i.InstrumentCode
        join jseweb_term_taxonomy itax on itax.term_id = it.term_id and itax.taxonomy = 'post_tag'
        join jseweb_term_relationships irel on irel.term_taxonomy_id = itax.term_taxonomy_id
     order by object_id
    ),
    
    cat as (select
        catt.name
        ,catt.slug
        ,catt.term_id
        ,crel.object_id
    from 
        jseweb_terms catt
        join jseweb_term_taxonomy cattax on cattax.term_id = catt.term_id 
            and cattax.taxonomy = 'category'
        join jseweb_term_relationships crel on crel.term_taxonomy_id = cattax.term_taxonomy_id
    where
        catt.slug in (
            'annual-reports', 
            'audited-financial-statements', 
            'quarterly-financial-statements'
        )
    )
select
    a.id
    ,inst.instrumentcode
    ,a.post_title
    ,a.post_name
    ,a.guid
    ,cat.name as category_name
    ,a.post_date
    ,p.post_status
from 
    jseweb_posts a
    join jseweb_posts p on a.post_parent = p.id
    join inst on inst.object_id = p.id
    join cat on cat.object_id = inst.object_id
where
    a.post_type = 'attachment'
    and a.post_mime_type in ('application/pdf', 'application/msword')
    and a.post_date > '2025-01-01'
order by post_date
"""

@task
def execute_mysql_query_over_ssh(**context):
    logger = LoggingMixin().log
    logger.info("Starting MySQL SSH tunnel task execution")
    
    # Get connection configurations from XCom
    logger.info("Fetching connection configurations")
    ti = context['task_instance']
    configs = ti.xcom_pull(task_ids='get_connection_configs')
    ssh_config = configs[0]  # First item in the returned tuple
    mysql_config = configs[1]  # Second item in the returned tuple
    
    # Log configuration details (excluding sensitive info)
    logger.info(f"SSH Config - Host: {ssh_config['ssh_host']}, Port: {ssh_config['ssh_port']}, User: {ssh_config['ssh_username']}")
    logger.info(f"MySQL Config - Host: {mysql_config['mysql_host']}, Port: {mysql_config['mysql_port']}, Database: {mysql_config['mysql_database']}")
    
    # Ensure SSH key has correct permissions
    if os.path.exists(ssh_config['ssh_private_key']):
        current_perms = oct(os.stat(ssh_config['ssh_private_key']).st_mode)[-3:]
        logger.info(f"Current SSH key permissions: {current_perms}")
        os.chmod(ssh_config['ssh_private_key'], 0o600)
        new_perms = oct(os.stat(ssh_config['ssh_private_key']).st_mode)[-3:]
        logger.info(f"Updated SSH key permissions: {new_perms}")
    else:
        logger.error(f"SSH key not found at {ssh_config['ssh_private_key']}")
        raise FileNotFoundError(f"SSH key not found at {ssh_config['ssh_private_key']}")

    logger.info("Establishing SSH tunnel")
    try:
        tunnel = SSHTunnelForwarder(
            (ssh_config['ssh_host'], ssh_config['ssh_port']),
            ssh_username=ssh_config['ssh_username'],
            ssh_pkey=ssh_config['ssh_private_key'],
            remote_bind_address=(mysql_config['mysql_host'], mysql_config['mysql_port']),
            set_keepalive=10.0  # Keep the tunnel alive
        )
        tunnel.start()
        logger.info(f"SSH tunnel established successfully. Local bind port: {tunnel.local_bind_port}")
        
        logger.info("Connecting to MySQL database")
        try:
            connection = pymysql.connect(
                host='localhost',
                port=tunnel.local_bind_port,
                user=mysql_config['mysql_user'],
                password=mysql_config['mysql_password'],
                database=mysql_config['mysql_database'],
                client_flag=pymysql.constants.CLIENT.LOCAL_FILES,
                connect_timeout=10
            )
            
            logger.info("Successfully connected to MySQL database")
            logger.info("Executing SQL query")
            df = pd.read_sql(QUERY, connection)
            
            row_count = len(df)
            logger.info(f"Query executed successfully. Retrieved {row_count} rows")
            
            # Convert datetime columns to string format
            for col in df.select_dtypes(include=['datetime64[ns]']).columns:
                df[col] = df[col].astype(str)
            
            # Store results in XCom for the next task
            context['task_instance'].xcom_push(key='query_results', value=df.to_dict())
            logger.info("Results stored in XCom successfully")
            
            # Print sample of results
            logger.info("Sample of query results (first 5 rows):")
            logger.info("\n" + str(df.head()))
            
            return f"Task completed successfully. Retrieved {row_count} rows of data."
            
        except Exception as e:
            logger.error(f"MySQL Error: {str(e)}")
            logger.error(f"MySQL Error Type: {type(e)}")
            raise
        finally:
            if 'connection' in locals():
                connection.close()
                logger.info("Database connection closed")
    except Exception as e:
        logger.error(f"SSH Tunnel Error: {str(e)}")
        logger.error(f"SSH Tunnel Error Type: {type(e)}")
        raise
    finally:
        if 'tunnel' in locals() and tunnel.is_active:
            tunnel.stop()
            logger.info("SSH tunnel closed")

@task(outlets=[bq_dataset])
def load_to_bigquery(**context):
    logger = LoggingMixin().log
    logger.info("Starting BigQuery load task")
    
    # Get data from XCom
    ti = context['task_instance']
    data_dict = ti.xcom_pull(task_ids='execute_mysql_query_over_ssh', key='query_results')
    df = pd.DataFrame.from_dict(data_dict)
    
    # Normalize column names to lowercase
    df.columns = df.columns.str.lower()
    logger.info(f"Normalized column names: {list(df.columns)}")
    
    # Define table schema
    schema = [
        bigquery.SchemaField("id", "INTEGER"),
        bigquery.SchemaField("instrumentcode", "STRING"),
        bigquery.SchemaField("post_title", "STRING"),
        bigquery.SchemaField("post_name", "STRING"),
        bigquery.SchemaField("guid", "STRING"),
        bigquery.SchemaField("category_name", "STRING"),
        bigquery.SchemaField("post_date", "TIMESTAMP"),
        bigquery.SchemaField("post_status", "STRING"),
    ]
    
    # Initialize BigQuery client
    bq_hook = BigQueryHook(gcp_conn_id='google_cloud_default')
    client = bq_hook.get_client()
    
    # Create dataset if it doesn't exist
    try:
        client.get_dataset(DATASET_ID)
        logger.info(f"Dataset {DATASET_ID} already exists")
    except Exception:
        dataset = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
        dataset.location = "US"
        client.create_dataset(dataset)
        logger.info(f"Created dataset {DATASET_ID}")
    
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    
    # Create table if it doesn't exist
    try:
        client.get_table(table_ref)
        logger.info(f"Table {TABLE_ID} already exists")
    except Exception:
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)
        logger.info(f"Created table {TABLE_ID}")
    
    # Fetch existing IDs from BigQuery
    logger.info("Fetching existing IDs from BigQuery table to identify new rows.")
    existing_ids_query = f"SELECT id FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`"
    try:
        existing_ids_df = client.query(existing_ids_query).to_dataframe()
        existing_ids_set = set(existing_ids_df['id'].astype(str))
        logger.info(f"Fetched {len(existing_ids_set)} existing IDs from BigQuery.")
    except Exception as e:
        logger.warning(f"Could not fetch existing IDs from BigQuery (table may be empty): {e}")
        existing_ids_set = set()
    
    # Filter for new rows only
    df['id'] = df['id'].astype(str)
    new_rows_df = df[~df['id'].isin(existing_ids_set)]
    logger.info(f"Identified {len(new_rows_df)} new rows to load.")
    
    if new_rows_df.empty:
        logger.info("No new rows to load to BigQuery.")
        raise AirflowSkipException("No new rows to load.")
    
    # Load data to BigQuery
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )
    
    try:
        # Convert post_date back to datetime for BigQuery
        new_rows_df['post_date'] = pd.to_datetime(new_rows_df['post_date'])
        
        # Load data
        job = client.load_table_from_dataframe(
            new_rows_df, table_ref, job_config=job_config
        )
        job.result()  # Wait for the job to complete
        
        logger.info(f"Loaded {len(new_rows_df)} new rows to BigQuery table {table_ref}")
        return f"Successfully loaded {len(new_rows_df)} new rows to BigQuery"
        
    except Exception as e:
        logger.error(f"Error loading to BigQuery: {str(e)}")
        raise

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'mysql_ssh_tunnel_dag',
    default_args=default_args,
    description='Extract MySQL data over SSH tunnel and load to BigQuery',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    
    # Get configurations
    config_task = get_connection_configs()
    
    # Extract data using SSH tunnel
    extract_task = execute_mysql_query_over_ssh()
    
    # Load data to BigQuery
    load_task = load_to_bigquery()
    
    # Set task dependencies
    config_task >> extract_task >> load_task 