from datetime import datetime, timezone

def normalize_valet_json(payload: dict, series_id: str) -> list[dict]:
    """
    Convert Valet observations JSON into long-format rows for BigQuery.
    Output keys match raw.boc_fx_observations schema.
    """
    now = datetime.now(timezone.utc).isoformat()

    rows = []
    for obs in payload.get("observations", []):
        # obs["d"] is date string; obs[series_id]["v"] is value string
        date_str = obs.get("d")
        series_obj = obs.get(series_id, {})
        val_str = series_obj.get("v")

        if not date_str or val_str in (None, ""):
            continue

        rows.append(
            {
                "series_id": series_id,
                "observation_date": date_str,     # BigQuery can parse YYYY-MM-DD as DATE
                "value": float(val_str),
                "ingested_at": now,               # BigQuery can parse ISO timestamp
                "source": "bank_of_canada_valet",
            }
        )
    return rows
