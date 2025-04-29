# Airflow DAGs

This directory contains the Airflow DAGs for the JSE Datasphere Pipeline. The DAGs are designed to work together using Airflow's Dataset-aware scheduling feature.

## DAG Overview

### 1. MySQL SSH Tunnel DAG (`mysql_ssh_tunnel_dag.py`)

**Purpose**: Extracts financial document data from MySQL through an SSH tunnel and loads it to BigQuery.

**Schedule**: Daily

**Tasks**:
1. `execute_mysql_query`: 
   - Establishes SSH tunnel to MySQL
   - Executes query to fetch financial documents
   - Stores results in XCom
2. `load_to_bigquery`:
   - Normalizes column names
   - Loads data to BigQuery table
   - Updates the BigQuery dataset marker

**Dataset Produced**:
- `bq://jse-datasphere/jse_seeds/financial_documents`

**Query Details**:
- Extracts instrument data and related financial documents
- Filters for specific document categories:
  - Annual reports
  - Audited financial statements
  - Quarterly financial statements
- Includes metadata such as post titles, dates, and URLs

### 2. BigQuery Downstream DAG (`bigquery_downstream_dag.py`)

**Purpose**: Processes data from BigQuery after it's been updated by the upstream DAG.

**Schedule**: Dataset-driven (runs when BigQuery table is updated)

**Tasks**:
1. `read_from_bigquery`:
   - Reads data from BigQuery
   - Logs row count and sample data
   - Can be extended for additional processing

**Dataset Consumed**:
- `bq://jse-datasphere/jse_seeds/financial_documents`

## Dataset-Aware Scheduling

The DAGs are linked using Airflow's Dataset feature:
1. The upstream DAG marks the BigQuery table as a dataset using `outlets=[bq_dataset]`
2. The downstream DAG uses `schedule=[bq_dataset]` to run when the dataset is updated
3. This ensures the downstream DAG only runs when new data is available

## Required Connections

1. **MySQL Connection** (`jse_mysql`):
   - Type: MySQL
   - Host: [MySQL host]
   - Schema: jsewebsite
   - Login: [MySQL username]
   - Password: [MySQL password]

2. **SSH Tunnel Connection** (`jse_ssh_tunnel`):
   - Type: SSH
   - Host: [SSH host]
   - Username: [SSH username]
   - Private Key: Located in `/opt/airflow/ssh_keys/id_rsa`

3. **BigQuery Connection** (`google_cloud_default`):
   - Type: Google Cloud Platform
   - Project: jse-datasphere
   - Keyfile Path: [Path to service account key]

## Testing

To test the DAGs individually:

```bash
# Test MySQL data extraction
airflow tasks test mysql_ssh_tunnel_dag execute_mysql_query 2024-03-28

# Test BigQuery load
airflow tasks test mysql_ssh_tunnel_dag load_to_bigquery 2024-03-28

# Test downstream BigQuery processing
airflow tasks test bigquery_downstream_dag read_from_bigquery 2024-03-28
```

## Monitoring

Monitor the DAGs in the Airflow UI:
1. Check the Dataset Dependencies view to see the relationship between DAGs
2. View task logs for detailed execution information
3. Monitor XCom for data passing between tasks
4. Check BigQuery table for data updates