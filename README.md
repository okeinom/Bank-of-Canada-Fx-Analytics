## Bank of Canada Fx Analytics

## Orchestration (Airflow)

This project includes an Apache Airflow DAG that orchestrates:
- FX data ingestion into BigQuery
- dbt run
- dbt test

The DAG is provided for production deployment (e.g., Cloud Composer).
Airflow is not required to be installed locally to run the core pipeline.