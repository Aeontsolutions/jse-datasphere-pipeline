FROM apache/airflow:2.7.3-python3.9

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Additional step to ensure packages are installed in the correct Python environment
RUN pip install pymysql pandas sshtunnel 

# Install additional provider packages for connection testing
RUN pip install \
    apache-airflow-providers-ssh \
    apache-airflow-providers-mysql \
    apache-airflow-providers-common-sql \
    paramiko 