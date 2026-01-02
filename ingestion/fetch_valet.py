import requests

BASE = "https://www.bankofcanada.ca/valet/observations"


def fetch_series(series_id: str, start_date: str, end_date: str) -> dict:
    """
    Fetch observations for a single Valet series.
    Dates are YYYY-MM-DD.
    """
    url = f"{BASE}/{series_id}"
    params = {"start_date": start_date, "end_date": end_date}
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()
