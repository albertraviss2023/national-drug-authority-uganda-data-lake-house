FROM apache/airflow:2.7.3

USER root

# Install system dependencies including Java and procps
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3-dev \
        default-libmysqlclient-dev \
        build-essential \
        freetds-dev \
        freetds-bin \
        libssl-dev \
        openjdk-11-jdk \
        pkg-config \
        procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME for spark-submit compatibility
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

USER airflow

# Add airflow user's pip bin to PATH
ENV PATH=/home/airflow/.local/bin:$PATH

# Upgrade pip and install Cython
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir Cython==0.29.36

# Install required Python packages
RUN pip3 install --no-cache-dir --upgrade pip && \
    pip3 install --no-cache-dir \
        apache-airflow-providers-apache-spark==4.1.0 \
        apache-airflow-providers-postgres==5.7.0 \
        psycopg2-binary==2.9.9 \
        pyspark==3.5.0 \
        apache-airflow-providers-microsoft-mssql==3.6.0 \
        apache-airflow-providers-apache-kafka==1.3.0 \
        apache-airflow-providers-trino==5.6.0 \
        papermill==2.6.0 \
        nbclient==0.10.0 \
        pyodbc==5.1.0 \
        confluent-kafka==2.5.0 \
        minio==7.2.8 \
        pymssql==2.2.11 \
        papermill \
        duckdb==1.1.2 \
        pyarrow>=14