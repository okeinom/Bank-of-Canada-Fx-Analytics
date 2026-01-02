from __future__ import annotations

import os
import subprocess
from datetime import datetime

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# --- Adjust if your repo name differs ---
# This assumes the DAG file lives at: <repo_root>/airflow/dags/boc_fx_pipeline_dag.py
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DBT_DIR = os.path.join(REPO_ROOT, "dbt", "boc_fx_analytics")  # after you run dbt init
INGEST_CMD = ["python", "-m", "ingestion.run_ingestion"]


def run_ingestion(**context):
    """
    Runs ingestion module.
    Supports two modes:
      - incremental (default)
      - backfill (manual trigger via params)
    """
    params = context.get("params", {}) or {}

    mode = params.get("mode", "incremental")
    series = params.get("series", ["FXUSDCAD", "FXEURCAD", "FXGBPCAD"])
    start_date = params.get("start_date")
    end_date = params.get("end_date")

    cmd = INGEST_CMD.copy()
    cmd += ["--mode", mode]
    cmd += ["--series"] + series

    if mode == "backfill":
        # Optional: if provided, pass through
        if start_date:
            cmd += ["--start-date", start_date]
        if end_date:
            cmd += ["--end-date", end_date]

    print(f"Running ingestion with command: {' '.join(cmd)}")
    subprocess.run(cmd, cwd=REPO_ROOT, check=True)


with DAG(
    dag_id="boc_fx_batch_pipeline",
    description="Bank of Canada FX → BigQuery raw → dbt staging/marts → tests",
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Toronto"),
    schedule="0 7 * * 1-5",  # 7:00am Mon-Fri (adjust later)
    catchup=False,
    default_args={"owner": "okeino"},
    params={
        # Default: incremental daily run
        "mode": "incremental",  # or "backfill"
        "series": ["FXUSDCAD", "FXEURCAD", "FXGBPCAD"],
        # Used only if mode == backfill
        "start_date": None,  # e.g. "2019-01-01"
        "end_date": None,    # e.g. "2026-01-02"
    },
    tags=["portfolio", "bank_of_canada", "bigquery", "dbt"],
) as dag:

    t1_ingest = PythonOperator(
        task_id="extract_load_boc_fx_to_bigquery_raw",
        python_callable=run_ingestion,
    )

    t2_dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="dbt run",
        cwd=DBT_DIR,
        env={
            **os.environ,
            # Ensures dbt uses the same environment variables as Airflow
        },
    )

    t3_dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="dbt test",
        cwd=DBT_DIR,
        env={**os.environ},
    )

    t1_ingest >> t2_dbt_run >> t3_dbt_test
