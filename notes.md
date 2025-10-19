docker compose exec airflow-scheduler bash -lc 'pip install --no-cache-dir openpyxl pyarrow fastparquet'
docker compose exec airflow-webserver bash -lc 'pip install --no-cache-dir openpyxl pyarrow fastparquet'


## dependencies
pip install --upgrade boto3==1.40.49 botocore==1.40.49 s3fs==2025.9.0 fsspec==2025.9.0 aiobotocore==2.25.0

## Env before streamlit run on machine
$env:AWS_ACCESS_KEY_ID    = "minioadmin"
$env:AWS_SECRET_ACCESS_KEY = "minioadmin123"
$env:AWS_REGION           = "us-east-1"
$env:CATALOG_WAREHOUSE    = "s3://warehouse"
$env:CATALOG_S3_ENDPOINT  = "http://localhost:9000"

## Activating the env
python -m venv .venv   
.\.venv\Scripts\activate       
