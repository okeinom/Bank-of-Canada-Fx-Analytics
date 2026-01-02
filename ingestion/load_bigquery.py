from google.cloud import bigquery

# Load rows into a staging table
def load_rows_to_stage(project_id: str, dataset_id: str, rows: list[dict]) -> None:
    if not rows:
        print("No rows to insert.")
        return

    client = bigquery.Client(project=project_id)
    stage_ref = f"{project_id}.{dataset_id}._fx_observations_stage"

    # Optional: clear stage each run
    client.query(f"TRUNCATE TABLE `{stage_ref}`").result()

    errors = client.insert_rows_json(stage_ref, rows)
    if errors:
        raise RuntimeError(f"BigQuery insert errors: {errors}")

    print(f"Loaded {len(rows)} rows into staging table {stage_ref}")


def get_max_loaded_date(project_id: str, dataset_id: str, table_id: str, series_id: str):
    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    query = f"""
    SELECT MAX(observation_date) AS max_date
    FROM `{table_ref}`
    WHERE series_id = @series_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("series_id", "STRING", series_id)]
    )
    rows = list(client.query(query, job_config=job_config).result())
    return rows[0]["max_date"]  # may be None if no data yet

def merge_stage_into_raw(project_id: str, dataset_id: str):
    client = bigquery.Client(project=project_id)

    raw_table = f"{project_id}.{dataset_id}.boc_fx_observations"
    stage_table = f"{project_id}.{dataset_id}._fx_observations_stage"

    sql = f"""
    MERGE `{raw_table}` T
    USING `{stage_table}` S
    ON T.series_id = S.series_id AND T.observation_date = S.observation_date
    WHEN MATCHED THEN
      UPDATE SET
        value = S.value,
        ingested_at = S.ingested_at,
        source = S.source
    WHEN NOT MATCHED THEN
      INSERT (series_id, observation_date, value, ingested_at, source)
      VALUES (S.series_id, S.observation_date, S.value, S.ingested_at, S.source)
    """

    client.query(sql).result()

