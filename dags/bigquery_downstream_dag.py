"""
DAG to read financial document metadata from BigQuery, filter PDFs by keyword, and save filtered results. Includes utilities for PDF processing and future data enrichment.
"""

# Standard library imports
import os
import time
import shutil
import base64
import tempfile
from datetime import datetime, timedelta
from urllib.parse import unquote
import json

# Third-party imports
import pandas as pd
import requests

# Airflow imports
from airflow import DAG
from airflow.decorators import task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.datasets import Dataset
from airflow.utils.log.logging_mixin import LoggingMixin

# Google Gemini imports
from google import genai
from google.genai import types

# ------------------- Utility Functions -------------------
def count_pdf_pages(pdf_path):
    """
    Count the number of pages in a PDF file.
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

def pdf_to_base64(pdf_path):
    """
    Convert a PDF file to a base64-encoded string.
    """
    with open(pdf_path, 'rb') as f:
        return base64.b64encode(f.read()).decode('utf-8')

def check_keywords_in_response(json_string, keywords):
    """
    Parse a JSON object and check if any of the specified keywords are in the 'response' field.
    """
    try:
        data = json.loads(json_string)
        if 'response' not in data:
            raise KeyError("The JSON object does not contain a 'response' field")
        response_text = data['response'].lower()
        return {keyword: keyword.lower() in response_text for keyword in keywords}
    except json.JSONDecodeError:
        raise ValueError("Invalid JSON string provided")
    except Exception as e:
        raise e

def string_to_json(json_string):
    """
    Convert a JSON string to a Python dictionary.
    """
    try:
        return json.loads(json_string)
    except Exception as e:
        print(f"Error decoding JSON: {e}")
        if isinstance(json_string, dict):
            print("Input was already a dictionary, returning as is.")
            return json_string
        return None

def generate(base64_data):
    """
    Use Gemini API to extract the title page content from a PDF (base64-encoded).
    """
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        raise ValueError("GEMINI_API_KEY environment variable not set.")
    client = genai.Client(api_key=api_key)
    document1 = types.Part.from_bytes(
        data=base64.b64decode(base64_data), mime_type="application/pdf",
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
        temperature=1,
        top_p=0.95,
        max_output_tokens=8192,
        response_modalities=["TEXT"],
        safety_settings=[
            types.SafetySetting(category="HARM_CATEGORY_HATE_SPEECH", threshold="OFF"),
            types.SafetySetting(category="HARM_CATEGORY_DANGEROUS_CONTENT", threshold="OFF"),
            types.SafetySetting(category="HARM_CATEGORY_SEXUALLY_EXPLICIT", threshold="OFF"),
            types.SafetySetting(category="HARM_CATEGORY_HARASSMENT", threshold="OFF")
        ],
        response_mime_type="application/json",
        response_schema={"type": "OBJECT", "properties": {"response": {"type": "STRING"}}},
    )
    json_string = ""
    for chunk in client.models.generate_content_stream(
        model=model,
        contents=contents,
        config=generate_content_config,
    ):
        json_string += chunk.text
    return json_string

def generate_date(document_path):
    """
    Use Gemini API to extract a date from the first page of a PDF.
    """
    base64_data = pdf_to_base64(document_path)
    client = genai.Client(
        vertexai=True,
        project="price-aggregator-f9e4b",
        location="us-central1",
    )
    document1 = types.Part.from_bytes(
        data=base64.b64decode(base64_data),
        mime_type="application/pdf",
    )
    model = "gemini-2.0-flash-001"
    contents = [
        types.Content(
            role="user",
            parts=[
                document1,
                types.Part.from_text(text="""From the document's first page extract date, month, year in the format dd-name_of_month-yyyy""")
            ]
        )
    ]
    generate_content_config = types.GenerateContentConfig(
        temperature=1,
        top_p=0.95,
        max_output_tokens=8192,
        response_modalities=["TEXT"],
        safety_settings=[
            types.SafetySetting(category="HARM_CATEGORY_HATE_SPEECH", threshold="OFF"),
            types.SafetySetting(category="HARM_CATEGORY_DANGEROUS_CONTENT", threshold="OFF"),
            types.SafetySetting(category="HARM_CATEGORY_SEXUALLY_EXPLICIT", threshold="OFF"),
            types.SafetySetting(category="HARM_CATEGORY_HARASSMENT", threshold="OFF")
        ],
        response_mime_type="application/json",
        response_schema={"type": "OBJECT", "properties": {"response": {"type": "STRING"}}},
    )
    json_string = ""
    for chunk in client.models.generate_content_stream(
        model=model,
        contents=contents,
        config=generate_content_config,
    ):
        json_string += chunk.text
    return string_to_json(json_string)['response']

def populate_csv_data(filtered_df, generate_date_function, output_path=None):
    """
    Populate missing columns in the CSV file with data from other columns and by processing PDF files.
    Saves the CSV incrementally after each PDF is processed.
    """
    # Read the CSV file
    df = filtered_df

    # Check if we're continuing from a previous run
    continuing = False
    if 'Company Name' in df.columns and df['Company Name'].notna().any():
        print("Found existing data in Company Name column. Continuing from where left off.")
        continuing = True

    # Populate static columns if not continuing
    if not continuing:
        df['Company Name'] = df['InstrumentName']
        df['Company Symbol'] = df['InstrumentCode']
        df['Statement Type'] = df['category']

        # Create Period column if it doesn't exist
        if 'Period' not in df.columns:
            print("Adding 'Period' column to the DataFrame")
            df['Period'] = None

        # Save the initial population
        df.to_csv(output_path, index=False)
        print(f"Initial column population complete. Saved to {output_path}")
    else:
        # Create Period column if it doesn't exist even if continuing
        if 'Period' not in df.columns:
            print("Adding 'Period' column to the DataFrame")
            df['Period'] = None
            df.to_csv(output_path, index=False)
            print(f"Added 'Period' column. Saved to {output_path}")

    # Create a temporary directory to store downloaded PDFs
    temp_dir = tempfile.mkdtemp()
    print(f"Created temporary directory: {temp_dir}")

    # Function to download PDF and extract period
    def process_pdf(guid, row_index):
        if not isinstance(guid, str) or not guid:
            return None

        try:
            # Decode URL if needed
            url = unquote(guid)

            # Fix URL if needed: ensure it starts with https:// and handle www. properly
            if "www." in url:
                www_pos = url.find("www.")
                if www_pos > 0 or not url.startswith("https://"):
                    url = "https://" + url[www_pos:]
            elif not url.startswith("http://") and not url.startswith("https://"):
                url = "https://" + url

            # Download the PDF
            print(f"Downloading PDF from {url}")
            response = requests.get(url, stream=True, timeout=30)
            if response.status_code != 200:
                print(f"Failed to download PDF from {url}, status code: {response.status_code}")
                return None

            # Generate a safe unique filename based on the hash of the URL
            filename = f"temp_{abs(hash(guid)) % 10000}.pdf"
            temp_pdf_path = os.path.join(temp_dir, filename)

            # Save to temp file
            with open(temp_pdf_path, 'wb') as f:
                f.write(response.content)

            print(f"PDF saved to {temp_pdf_path}")

            # Extract period using the provided function
            try:
                print("Calling generate_date function...")
                period = generate_date_function(temp_pdf_path)
                print(f"Raw generate_date result: {period}")

                # If period is a dictionary or JSON string, extract just the response value
                if isinstance(period, dict) and 'response' in period:
                    period = period['response']
                elif isinstance(period, str) and '{"response":' in period:
                    try:
                        import json
                        period_data = json.loads(period)
                        period = period_data.get('response')
                    except Exception as json_error:
                        print(f"Error parsing JSON response: {str(json_error)}")
                        # Keep original value if parsing fails

                print(f"Extracted period: {period}")
            except Exception as api_error:
                print(f"Error calling generate_date function: {str(api_error)}")
                period = None

            # Clean up the temp file
            try:
                os.remove(temp_pdf_path)
                print(f"Removed temporary file {temp_pdf_path}")
            except Exception as rm_error:
                print(f"Warning: Could not remove temp file {temp_pdf_path}: {str(rm_error)}")

            # Update the DataFrame and save incrementally
            if period is not None:
                df.at[row_index, 'Period'] = period
                # Save after each successful processing
                df.to_csv(output_path, index=False)
                print(f"Updated and saved CSV file with period: {period} for row {row_index}")

            return period

        except Exception as e:
            print(f"Error processing PDF from {guid}: {str(e)}")
            return None

    # Determine which rows need processing
    rows_to_process = []
    for i in range(len(df)):
        if pd.isna(df['Period'].iloc[i]) and isinstance(df['guid'].iloc[i], str):
            rows_to_process.append(i)

    total_to_process = len(rows_to_process)
    print(f"Found {total_to_process} rows that need processing")

    # Process a small batch first to test
    sample_size = min(5, total_to_process)
    if sample_size > 0:
        print(f"Testing on first {sample_size} PDFs...")

        for i in range(sample_size):
            row_idx = rows_to_process[i]
            guid = df['guid'].iloc[row_idx]
            print(f"\nProcessing sample {i+1}/{sample_size} (row {row_idx})...")
            period = process_pdf(guid, row_idx)
            print(f"Sample {i+1} result: {period}")
            # Add a small delay between API calls
            if i < sample_size - 1:
                time.sleep(2)
    else:
        print("No rows need processing.")
        shutil.rmtree(temp_dir)
        return df

    # Ask for confirmation to continue if there are more rows
    if total_to_process > sample_size:
        confirmation = input(f"Processed {sample_size} PDFs. Continue with remaining {total_to_process - sample_size} rows? (y/n): ")

        if confirmation.lower() == 'y':
            # Process the rest of the rows
            for i in range(sample_size, total_to_process):
                row_idx = rows_to_process[i]
                guid = df['guid'].iloc[row_idx]
                print(f"\nProcessing row {i+1}/{total_to_process} (CSV row {row_idx})...")
                period = process_pdf(guid, row_idx)
                print(f"Result: {period}")
                # Add a small delay between API calls to avoid rate limiting
                if i < total_to_process - 1:
                    time.sleep(2)
        else:
            print("Processing stopped by user after sample.")
    else:
        print("All rows have been processed in the sample.")

    # Clean up the temp directory - use shutil which handles non-empty directories
    try:
        shutil.rmtree(temp_dir)
        print(f"Removed temporary directory {temp_dir}")
    except Exception as e:
        print(f"Warning: Could not remove temp directory: {str(e)}")

    print(f"Processing complete. CSV saved to {output_path}")
    return df

# ------------------- Airflow Task Functions -------------------
PROJECT_ID = "jse-datasphere"
DATASET_ID = "jse_seeds"
TABLE_ID = "financial_documents"

bq_dataset = Dataset("bq://jse-datasphere/jse_seeds/financial_documents")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@task
def read_from_bigquery(**context):
    """
    Read new rows from BigQuery for the current DAG run interval.
    """
    logger = LoggingMixin().log
    bq_hook = BigQueryHook(gcp_conn_id='google_cloud_default')
    client = bq_hook.get_client()
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    start = context['data_interval_start']
    end = context['data_interval_end']
    if start == end:
        start = end - timedelta(days=1)
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
    Filter a DataFrame based on whether PDFs referenced by GUIDs contain a specific keyword.
    """
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
    """
    Save the filtered DataFrame to a CSV file.
    """
    logger = LoggingMixin().log
    if not filtered_df.empty:
        output_path = '/tmp/filtered_financial_statements.csv'
        filtered_df.to_csv(output_path, index=False)
        logger.info(f"Filtered DataFrame saved to {output_path} with {len(filtered_df)} rows.")
    else:
        logger.warning("No rows matched the keyword. No file saved.")
    return True

# ------------------- Airflow DAG Definition -------------------
with DAG(
    'bigquery_downstream_dag',
    default_args=default_args,
    description='Reads from BigQuery after upstream DAG completes',
    schedule=[bq_dataset],  # Dataset-aware scheduling
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    # Task 1: Read from BigQuery
    read_task = read_from_bigquery()
    # Task 2: Filter by PDF keyword
    filtered_df = filter_csv_by_pdf_keyword(read_task, 'audited')
    # Task 3: Save filtered results
    filter_and_save = filter_and_save_task(filtered_df)
    # Placeholder: Future task for data enrichment (e.g., populate_csv_data)
    # enrich_task = enrich_csv_data(filtered_df)