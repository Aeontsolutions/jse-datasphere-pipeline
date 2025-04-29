## DAG Details

### MySQL SSH Tunnel DAG (`mysql_ssh_tunnel_dag.py`)

This DAG establishes an SSH tunnel to securely connect to the MySQL database, extract data, and load it into BigQuery.

**Schedule**: Daily

**Tasks**:
1. `execute_mysql_query`: Establishes SSH tunnel and executes data extraction query
2. `load_to_bigquery`: Loads the extracted data into BigQuery

**Data Flow**:
1. Extracts instrument data and related financial documents from MySQL
2. Transforms column names to match BigQuery schema
3. Loads data into BigQuery table (`jse-datasphere.jse_seeds.financial_documents`)

**Query Details**:
- Extracts instrument data and related financial documents
- Filters for specific document categories:
  - Annual reports
  - Audited financial statements
  - Quarterly financial statements
- Includes metadata such as post titles, dates, and URLs