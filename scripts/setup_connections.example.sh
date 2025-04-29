#!/bin/bash

# MySQL Connection Details
MYSQL_HOST="localhost"
MYSQL_DB="your_database"
MYSQL_USER="your_username"
MYSQL_PASSWORD="your_password"
MYSQL_PORT="3306"

# SSH Tunnel Details
SSH_HOST="your.ssh.host"  # The remote server IP
SSH_USER="your_ssh_user"
SSH_PORT="22"
SSH_KEY_PATH="/opt/airflow/ssh_keys/id_rsa"

# BigQuery Details
GCP_PROJECT_ID="your-project-id"
# Read GCP credentials from file
GCP_CREDENTIALS=$(cat config/credentials.json)

# Create MySQL Connection
echo "Creating MySQL connection..."
docker compose run --rm airflow-webserver airflow connections add 'jse_mysql' \
    --conn-type 'mysql' \
    --conn-host "$MYSQL_HOST" \
    --conn-schema "$MYSQL_DB" \
    --conn-login "$MYSQL_USER" \
    --conn-password "$MYSQL_PASSWORD" \
    --conn-port "$MYSQL_PORT"

# Create SSH Tunnel Connection
echo "Creating SSH tunnel connection..."
docker compose run --rm airflow-webserver airflow connections add 'jse_ssh_tunnel' \
    --conn-type 'ssh' \
    --conn-host "$SSH_HOST" \
    --conn-login "$SSH_USER" \
    --conn-port "$SSH_PORT" \
    --conn-private-key "$SSH_KEY_PATH"

# Create BigQuery Connection
echo "Creating BigQuery connection..."
docker compose run --rm airflow-webserver airflow connections add 'google_cloud_default' \
    --conn-type 'google_cloud_platform' \
    --conn-extra "{\"project\": \"$GCP_PROJECT_ID\", \"keyfile_dict\": $GCP_CREDENTIALS}"

echo "All connections have been created!"
echo "Please verify the connections in the Airflow UI." 