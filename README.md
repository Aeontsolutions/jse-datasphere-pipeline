# JSE Datasphere Pipeline

This project contains Apache Airflow DAGs for extracting and processing data from the JSE (Jamaica Stock Exchange) database through a secure SSH tunnel.

## Overview

The pipeline currently includes:
- MySQL data extraction through SSH tunneling
- Automated document and financial statement processing
- Secure handling of database connections and SSH authentication
- Automated data loading to BigQuery for analytics

## Prerequisites

- Docker and Docker Compose
- Access to JSE MySQL database
- SSH access to the database server
- Required SSH keys for authentication
- Google Cloud Platform account with BigQuery access

## Project Structure

```
jse-datasphere-pipeline/
├── dags/                          # Airflow DAG files
├── logs/                          # Airflow logs directory
├── plugins/                       # Custom Airflow plugins
├── ssh_keys/                      # SSH keys for tunnel authentication
├── docker-compose.yml             # Docker Compose configuration
├── env.template                   # Template for environment variables
└── README.md                      # This file
```

## Setup Instructions

1. **Clone the Repository**
   ```bash
   git clone [repository-url]
   cd jse-datasphere-pipeline
   ```

2. **Environment Setup**
   ```bash
   # Create your environment file
   cp env.template .env
   
   # Generate a Fernet key for encryption
   python -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())'
   
   # Update the .env file with your values:
   # - Add the generated Fernet key
   # - Set database credentials
   # - Configure admin user details
   # - Adjust other settings as needed
   ```

3. **SSH Key Configuration**
   - Place your SSH private key in the `ssh_keys` directory
   - Ensure the key is named `id_rsa`
   - Set proper permissions:
     ```bash
     chmod 600 ssh_keys/id_rsa
     ```

4. **Start the Environment**
   ```bash
   docker compose up -d
   ```

5. **Airflow Connections Setup**
   
   The project includes an automated connection setup script. To use it:

   a. **Create Required Directories**
   ```bash
   mkdir -p config ssh_keys
   ```

   b. **Set Up Credentials**
   - Copy your GCP service account JSON to `config/credentials.json`
   - Place your SSH private key in `ssh_keys/id_rsa`
   - Set proper permissions:
     ```bash
     chmod 600 ssh_keys/id_rsa
     ```

   c. **Prepare Connection Script**
   ```bash
   # Copy the example script
   cp scripts/setup_connections.example.sh scripts/setup_connections.sh
   
   # Make it executable
   chmod +x scripts/setup_connections.sh
   ```

   d. **Update Connection Details**
   Edit `scripts/setup_connections.sh` and update the following variables with your values:
   ```bash
   # MySQL Connection Details
   MYSQL_HOST="your_host"
   MYSQL_DB="your_database"
   MYSQL_USER="your_username"
   MYSQL_PASSWORD="your_password"
   
   # SSH Tunnel Details
   SSH_HOST="your.ssh.host"
   SSH_USER="your_ssh_user"
   
   # BigQuery Details
   GCP_PROJECT_ID="your-project-id"
   ```

   e. **Run the Setup Script**
   ```bash
   ./scripts/setup_connections.sh
   ```

   This will create all necessary connections:
   - MySQL Connection (`jse_mysql`)
   - SSH Tunnel Connection (`jse_ssh_tunnel`)
   - BigQuery Connection (`google_cloud_default`)

   Verify the connections in the Airflow UI after running the script.

## Environment Variables

Key environment variables in `.env`:

```bash
# Core Settings
AIRFLOW_FERNET_KEY=                # Required for encryption
AIRFLOW__CORE__EXECUTOR=CeleryExecutor
AIRFLOW__CORE__LOAD_EXAMPLES=false
AIRFLOW__CORE__SECURE_MODE=true

# Database Settings
POSTGRES_USER=airflow
POSTGRES_PASSWORD=<your-password>
POSTGRES_DB=airflow

# Redis Settings
REDIS_PASSWORD=<optional-password>

# Admin User
ADMIN_USERNAME=admin
ADMIN_PASSWORD=<your-password>
ADMIN_EMAIL=admin@example.com

# Additional Settings
AIRFLOW__CORE__ALLOWED_DESERIALIZATION_CLASSES=('airflow.providers.mysql.hooks.mysql.MySqlHook', 'airflow.providers.ssh.hooks.ssh.SSHHook')
```

## Monitoring and Maintenance

1. **Access Airflow UI**
   - URL: http://localhost:8080
   - Default credentials:
     - Username: admin
     - Password: admin

2. **View Logs**
   ```bash
   docker compose logs -f airflow-worker
   ```

3. **Check DAG Status**
   ```bash
   docker compose run --rm airflow-worker airflow dags list
   ```

4. **Test DAG**
   ```bash
   docker compose run --rm airflow-worker airflow tasks test mysql_ssh_tunnel_dag execute_mysql_query [date]
   ```

## Troubleshooting

1. **SSH Tunnel Issues**
   - Verify SSH key permissions (must be 600)
   - Ensure key is mounted correctly in Docker container
   - Check SSH connection details in Airflow connection

2. **MySQL Connection Issues**
   - Verify MySQL credentials
   - Confirm database access permissions
   - Check if MySQL host is reachable through SSH tunnel

3. **Docker Issues**
   - Ensure all required services are running:
     ```bash
     docker compose ps
     ```
   - Check service logs:
     ```bash
     docker compose logs [service-name]
     ```

## Security Considerations

1. **SSH Keys**
   - Never commit SSH keys to version control
   - Maintain strict file permissions
   - Regularly rotate keys according to security policy

2. **Database Credentials**
   - Use Airflow connections to manage credentials
   - Avoid hardcoding sensitive information
   - Regularly update passwords

3. **Access Control**
   - Implement proper Airflow RBAC if needed
   - Restrict network access to necessary services
   - Monitor access logs

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request
