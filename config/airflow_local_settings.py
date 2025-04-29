"""Custom Airflow configuration."""

# Enable connection test button in the UI
AIRFLOW__CORE__TEST_CONNECTION = True

# Additional security settings
AIRFLOW__CORE__SECURE_MODE = True
AIRFLOW__WEBSERVER__EXPOSE_CONFIG = False
AIRFLOW__CORE__FERNET_KEY = 'dKJYtTNPQOOSuhUFqiIuTXLIJiBlsw71AhLN5xRuZso='  # Match docker-compose.yml

# Enable connection testing for these conn types
AIRFLOW__CORE__TEST_CONNECTION_ENABLE_EXAMPLES = True
AIRFLOW__CORE__ALLOWED_DESERIALIZATION_CLASSES = (
    'airflow.providers.mysql.hooks.mysql.MySqlHook',
    'airflow.providers.ssh.hooks.ssh.SSHHook',
) 