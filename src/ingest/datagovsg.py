"""Ingest MAS data from data.gov.sg API.

Singapore's data.gov.sg provides some MAS datasets via API.
Currently available: Exchange rates (SGD/USD daily and annual).

Source: https://data.gov.sg/
API docs: https://guide.data.gov.sg/developer-guide/dataset-apis
"""

from subsets_utils import get, save_raw_json, load_state, save_state
from urllib.parse import urlencode


# Known MAS datasets on data.gov.sg
DATASETS = {
    "exchange_rates_usd_daily": "d_046ff8d521a218d9178178cfbfc45c2c",
    "exchange_rates_usd_annual": "d_6cb7c12d5f25f0a04e70657dfebcb514",
}

BASE_URL = "https://api-production.data.gov.sg/v2/public/api/datasets"


def fetch_all_rows(dataset_id, limit=1000):
    """Fetch all rows from a dataset using pagination."""
    all_rows = []
    cursor = None

    while True:
        url = f"{BASE_URL}/{dataset_id}/list-rows?limit={limit}"
        if cursor:
            url = f"{url}&{cursor}"

        response = get(url, timeout=60.0)
        response.raise_for_status()
        data = response.json()

        rows = data.get("data", {}).get("rows", [])
        all_rows.extend(rows)

        # Check for next page
        next_cursor = data.get("data", {}).get("links", {}).get("next")
        if not next_cursor or not rows:
            break

        cursor = next_cursor
        print(f"      Fetched {len(all_rows)} rows so far...")

    return all_rows


def run():
    """Fetch MAS datasets from data.gov.sg API."""
    state = load_state("datagovsg")
    completed = set(state.get("completed", []))

    pending = [(name, dataset_id) for name, dataset_id in DATASETS.items() if name not in completed]

    if not pending:
        print("  All data.gov.sg datasets up to date")
        return

    print(f"  Fetching {len(pending)} datasets from data.gov.sg...")

    for i, (name, dataset_id) in enumerate(pending, 1):
        print(f"  [{i}/{len(pending)}] Fetching {name}...")

        # Fetch metadata
        meta_url = f"{BASE_URL}/{dataset_id}/metadata"
        meta_response = get(meta_url, timeout=60.0)
        meta_response.raise_for_status()
        metadata = meta_response.json()

        # Fetch all rows using pagination
        rows = fetch_all_rows(dataset_id)

        save_raw_json({
            "metadata": metadata.get("data", {}),
            "rows": rows
        }, name)

        completed.add(name)
        save_state("datagovsg", {"completed": list(completed)})

        print(f"    -> saved {name} ({len(rows)} rows)")

    print(f"  Completed: {len(completed)}/{len(DATASETS)} datasets")
