from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
import pandas as pd
from airflow.datasets import Dataset
from airflow.utils.log.logging_mixin import LoggingMixin
import requests
import base64
import tempfile
import os
from google import genai
from google.genai import types
import json
from airflow.operators.python import get_current_context

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

    # If start and end are the same, adjust start to be 1 day before end (for manual runs)
    if start == end:
        start = end - timedelta(days=1)

    # Format for BigQuery
    start_str = start.strftime('%Y-%m-%dT%H:%M:%S')
    end_str = end.strftime('%Y-%m-%dT%H:%M:%S')

    logger.info(f"Reading rows from {start_str} to {end_str}")
    query = f'''
        SELECT * FROM `{table_ref}`
        WHERE loaded_at >= TIMESTAMP('{start_str}') AND loaded_at < TIMESTAMP('{end_str}')
        limit 10
    '''
    df = bq_hook.get_pandas_df(sql=query, dialect='standard')
    row_count = len(df)
    context['ti'].log.info(f"Read {row_count} new rows from {table_ref}")
    context['ti'].log.info(f"Sample data:\n{df.head()}")
    return df

@task
def filter_csv_by_pdf_keyword(df, keyword):
    """
    Filter a CSV file based on whether PDFs referenced by GUIDs contain a specific keyword.
    Args:
        df (pd.DataFrame): DataFrame containing the data
        keyword (str): Keyword to search for in the PDFs
    Returns:
        pd.DataFrame: DataFrame containing the filtered rows
    """
    # Create a temporary directory for storing PDFs
    with tempfile.TemporaryDirectory() as temp_dir:
        filtered_rows = []
        for index, row in df.iterrows():
            try:
                guid = row['guid']
                if not guid or pd.isna(guid):
                    print(f"Skipping row {index}: No valid GUID")
                    continue
                pdf_url = guid
                if "www." in pdf_url:
                    www_pos = pdf_url.find("www.")
                    if www_pos > 0 or not pdf_url.startswith("https://"):
                        pdf_url = "https://" + pdf_url[www_pos:]
                pdf_path = os.path.join(temp_dir, f"{index}.pdf")
                print(f"Downloading PDF for row {index} (GUID: {guid})...")
                response = requests.get(pdf_url)
                if response.status_code != 200:
                    print(f"Failed to download PDF for row {index}: HTTP {response.status_code}")
                    continue
                with open(pdf_path, 'wb') as f:
                    f.write(response.content)
                base64_data = pdf_to_base64(pdf_path)
                pages = count_pdf_pages(pdf_path)
                json_string = generate(base64_data)
                result = check_keywords_in_response(json_string, [keyword])
                if result[keyword] or pages > 10:
                    print(f"Keyword '{keyword}' found in row {index} (GUID: {guid}) with {pages}")
                    filtered_rows.append(row)
                else:
                    print(f"Keyword '{keyword}' NOT found in row {index} (GUID: {guid}) and page # is {pages}")
            except Exception as e:
                print(f"Error processing row {index}: {e}")
        filtered_df = pd.DataFrame(filtered_rows)
        if not filtered_df.empty:
            return filtered_df
        else:
            print(f"No rows matched the keyword '{keyword}'. Output file not created.")
            return pd.DataFrame()

@task
def filter_and_save_task(filtered_df):
    logger = LoggingMixin().log
    if not filtered_df.empty:
        output_path = '/tmp/filtered_financial_statements.csv'
        filtered_df.to_csv(output_path, index=False)
        logger.info(f"Filtered DataFrame saved to {output_path} with {len(filtered_df)} rows.")
    else:
        logger.warning("No rows matched the keyword. No file saved.")
    return True

def count_pdf_pages(pdf_path):
    """
    Count the number of pages in a PDF file.

    Args:
        pdf_path (str): Path to the PDF file

    Returns:
        int: Number of pages in the PDF file

    Raises:
        FileNotFoundError: If the PDF file does not exist
        Exception: If there's an error reading the PDF file
    """
    try:
        import PyPDF2

        with open(pdf_path, 'rb') as file:
            pdf_reader = PyPDF2.PdfReader(file)
            num_pages = len(pdf_reader.pages)

        return num_pages

    except FileNotFoundError:
        raise FileNotFoundError(f"The file {pdf_path} was not found.")
    except Exception as e:
        raise Exception(f"Error reading the PDF file: {e}")
    
def generate(base64_data):
  api_key = os.environ.get("GEMINI_API_KEY")
  if not api_key:
      raise ValueError("GEMINI_API_KEY environment variable not set.")
  client = genai.Client(api_key=api_key)

  document1 = types.Part.from_bytes(
      data=base64.b64decode(base64_data) ,mime_type="application/pdf",
  )

  model = "gemini-2.0-flash-001"
  contents = [
    types.Content(
      role="user",
      parts=[
        document1,
        types.Part.from_text(text="""What is on the title page of this document""")
      ]
    )
  ]
  generate_content_config = types.GenerateContentConfig(
    temperature = 1,
    top_p = 0.95,
    max_output_tokens = 8192,
    response_modalities = ["TEXT"],
    safety_settings = [types.SafetySetting(
      category="HARM_CATEGORY_HATE_SPEECH",
      threshold="OFF"
    ),types.SafetySetting(
      category="HARM_CATEGORY_DANGEROUS_CONTENT",
      threshold="OFF"
    ),types.SafetySetting(
      category="HARM_CATEGORY_SEXUALLY_EXPLICIT",
      threshold="OFF"
    ),types.SafetySetting(
      category="HARM_CATEGORY_HARASSMENT",
      threshold="OFF"
    )],
    response_mime_type = "application/json",
    response_schema = {"type":"OBJECT","properties":{"response":{"type":"STRING"}}},
  )
  json_string=""
  for chunk in client.models.generate_content_stream(
    model = model,
    contents = contents,
    config = generate_content_config,
    ):

      json_string=json_string+chunk.text
  return json_string

def check_keywords_in_response(json_string, keywords):
    """
    Parse a JSON object and check if any of the specified keywords are in the 'response' field.

    Args:
        json_string (str): A JSON string containing a 'response' field
        keywords (list): List of keywords to search for in the response

    Returns:
        dict: A dictionary with each keyword as key and True/False as value indicating presence
    """
    try:
        # Parse the JSON string
        data = json.loads(json_string)

        # Check if 'response' key exists in the JSON object
        if 'response' not in data:
            raise KeyError("The JSON object does not contain a 'response' field")

        # Get the response text
        response_text = data['response'].lower()

        # Check for each keyword in the response
        result = {}
        for keyword in keywords:
            result[keyword] = keyword.lower() in response_text

        return result

    except json.JSONDecodeError:
        raise ValueError("Invalid JSON string provided")
    except Exception as e:
        raise e

def pdf_to_base64(pdf_path):
    # Implementation for converting PDF to base64
    with open(pdf_path, 'rb') as f:
        return base64.b64encode(f.read()).decode('utf-8')

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
    filtered_df = filter_csv_by_pdf_keyword(read_task, 'audited')
    filter_and_save = filter_and_save_task(filtered_df)