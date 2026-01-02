from __future__ import annotations

import argparse
from datetime import date, timedelta
from typing import Iterable

from ingestion.fetch_valet import fetch_series
from ingestion.normalize import normalize_valet_json
from ingestion.load_bigquery import (
    get_max_loaded_date,
    load_rows_to_stage,
    merge_stage_into_raw,
)

PROJECT_ID = "boc-fx-analytics"
DATASET_ID = "raw"
TABLE_ID = "boc_fx_observations"

DEFAULT_SERIES = ["FXUSDCAD", "FXEURCAD", "FXGBPCAD"]
DEFAULT_BACKFILL_START = "2019-01-01"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Ingest Bank of Canada FX data into BigQuery (raw).")
    p.add_argument(
        "--series",
        nargs="+",
        default=DEFAULT_SERIES,
        help="One or more Valet series IDs (e.g., FXUSDCAD FXEURCAD). Default: 3 major CAD pairs.",
    )
    p.add_argument(
        "--mode",
        choices=["incremental", "backfill"],
        default="incremental",
        help="incremental (default) loads new dates only; backfill loads a fixed date range.",
    )
    p.add_argument(
        "--start-date",
        default=None,
        help="Backfill start date (YYYY-MM-DD). If omitted in backfill mode, defaults to 2019-01-01.",
    )
    p.add_argument(
        "--end-date",
        default=None,
        help="Backfill end date (YYYY-MM-DD). If omitted, defaults to today.",
    )
    return p.parse_args()


def run_for_series(series_id: str, start_date: str, end_date: str) -> int:
    print(f"\nLoading {series_id} from {start_date} to {end_date}...")

    payload = fetch_series(series_id, start_date, end_date)
    rows = normalize_valet_json(payload, series_id)

    if rows:
        load_rows_to_stage(PROJECT_ID, DATASET_ID, rows)
        merge_stage_into_raw(PROJECT_ID, DATASET_ID)
        return len(rows)

    print("No new rows; skipping stage/merge.")
    return 0


def main():
    args = parse_args()

    # Resolve date range for backfill
    backfill_start = args.start_date or DEFAULT_BACKFILL_START
    backfill_end = args.end_date or date.today().isoformat()

    total = 0

    for series_id in args.series:
        if args.mode == "backfill":
            start_date = backfill_start
            end_date = backfill_end
        else:
            max_date = get_max_loaded_date(PROJECT_ID, DATASET_ID, TABLE_ID, series_id)
            if max_date:
                start_date = (max_date + timedelta(days=1)).isoformat()
            else:
                # First-time load for this series: do an initial historical backfill
                start_date = DEFAULT_BACKFILL_START
            end_date = date.today().isoformat()

        total += run_for_series(series_id, start_date, end_date)

    print(f"\nDone. Total rows processed this run: {total}")


if __name__ == "__main__":
    main()
