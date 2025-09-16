-- ./postgres/init.sql
-- Create the prod_db database (if not already created by POSTGRES_DB)
CREATE DATABASE IF NOT EXISTS prod_db;

-- Grant privileges to the airflow user
GRANT ALL PRIVILEGES ON DATABASE prod_db TO airflow;

-- Optional: Create a schema for Airflow (Airflow will create its own tables)
CREATE SCHEMA IF NOT EXISTS airflow_schema;
ALTER SCHEMA airflow_schema OWNER TO airflow;