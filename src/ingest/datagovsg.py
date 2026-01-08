"""Ingest MAS data from data.gov.sg API.

Singapore's data.gov.sg provides MAS datasets via API.
This covers exchange rates, money supply, loans, interest rates, and related financial data.

Source: https://data.gov.sg/
API docs: https://guide.data.gov.sg/developer-guide/dataset-apis
"""

from subsets_utils import get, save_raw_json, load_state, save_state
from subsets_utils.environment import get_data_dir
import gzip
import json
import os
import time


# Threshold for streaming vs in-memory fetch
STREAM_THRESHOLD = 50000

# MAS and MAS-sourced datasets on data.gov.sg
# Format: "local_name": "dataset_id"
DATASETS = {
    # === Exchange Rates (MAS) ===
    "exchange_rates_usd_daily": "d_046ff8d521a218d9178178cfbfc45c2c",
    "exchange_rates_usd_annual": "d_6cb7c12d5f25f0a04e70657dfebcb514",

    # === Exchange Rates (SINGSTAT, sourced from MAS) ===
    "exchange_rates_avg_annual": "d_b09aeaf8eb591c4bfe347b66148c6b53",
    "exchange_rates_avg_monthly": "d_b2b7ffe00aaec3936ed379369fdf531b",
    "exchange_rates_avg_monthly_alt": "d_3c62d5eed03c40aeafbb6d0fa324e976",

    # === Interest Rates ===
    "bank_interest_rates_monthly": "d_5fe5a4bb4a1ecc4d8a56a095832e2b24",

    # === Money Supply ===
    "money_supply_monthly": "d_7ed3eccba609ac0bdfcf406d939bdb0b",
    "money_supply_historical_monthly": "d_4c6bd8b2c4aa7041a31f3ed0cd122c47",

    # === Currency ===
    "currency_in_circulation_monthly": "d_10036483fced016b239ce7d2ab175125",

    # === Commercial Banks - Loans ===
    "commercial_banks_loans_quarterly": "d_0396bc943075a37d44c720ceb5be660a",
    "commercial_banks_loans_monthly": "d_af0415517a3a3a94b3b74039934ef976",
    "total_loans_non_bank_customers": "d_c2e116320c9d36f6ea6cdd82fb763de2",

    # === Finance Companies ===
    "finance_companies_loans_monthly": "d_4f73f4471a84f944ed37b651a8227ad8",

    # === Foreign Exchange Market ===
    "fx_market_turnover_monthly": "d_6dd6162d59737d67edfb35026dfd58c2",

    # === Government Debt ===
    "govt_debt_by_maturity_annual": "d_fd4b8728cb059c04fc0322199f4b2696",
    "govt_debt_by_instrument_annual": "d_d4f7c9d15692b3c08aa9bc8bc56c0a72",

    # === Credit Cards ===
    "credit_charge_cards_annual": "d_b40deadbdc470e97b9e16de99c5e6ee2",
}

BASE_URL = "https://api-production.data.gov.sg/v2/public/api/datasets"


def fetch_batch(dataset_id, cursor=None, limit=1000):
    """Fetch a single batch of rows from a dataset."""
    url = f"{BASE_URL}/{dataset_id}/list-rows?limit={limit}"
    if cursor:
        url = f"{url}&{cursor}"

    response = get(url, timeout=60.0)
    response.raise_for_status()
    data = response.json()

    rows = data.get("data", {}).get("rows", [])
    next_cursor = data.get("data", {}).get("links", {}).get("next")

    return rows, next_cursor


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
        metadata = meta_response.json().get("data", {})

        # Check first batch to determine size
        first_batch, next_cursor = fetch_batch(dataset_id, limit=STREAM_THRESHOLD)

        if next_cursor and len(first_batch) == STREAM_THRESHOLD:
            # Large dataset - stream to NDJSON
            print(f"    Large dataset detected, streaming to NDJSON...")

            # Write first batch then continue streaming
            raw_dir = os.path.join(get_data_dir(), "raw")
            os.makedirs(raw_dir, exist_ok=True)
            output_path = os.path.join(raw_dir, f"{name}.ndjson.gz")
            meta_path = os.path.join(raw_dir, f"{name}_metadata.json")

            total_rows = 0
            cursor = next_cursor

            with gzip.open(output_path, 'wt', encoding='utf-8') as f:
                # Write first batch
                for row in first_batch:
                    f.write(json.dumps(row) + '\n')
                total_rows = len(first_batch)
                print(f"      Streamed {total_rows} rows...")

                # Continue with remaining pages
                while cursor:
                    rows, next_cursor = fetch_batch(dataset_id, cursor)

                    for row in rows:
                        f.write(json.dumps(row) + '\n')

                    total_rows += len(rows)

                    if total_rows % 50000 == 0:
                        print(f"      Streamed {total_rows} rows...")

                    if not next_cursor or not rows:
                        break

                    cursor = next_cursor
                    time.sleep(0.2)

            # Save metadata separately
            with open(meta_path, 'w', encoding='utf-8') as f:
                json.dump(metadata, f)

            print(f"    -> saved {name}.ndjson.gz ({total_rows} rows)")

        else:
            # Small dataset - accumulate in memory and save as JSON
            all_rows = first_batch
            cursor = next_cursor

            while cursor:
                rows, next_cursor = fetch_batch(dataset_id, cursor)
                all_rows.extend(rows)

                if not next_cursor or not rows:
                    break

                cursor = next_cursor
                time.sleep(0.2)

            save_raw_json({
                "metadata": metadata,
                "rows": all_rows
            }, name)

            print(f"    -> saved {name} ({len(all_rows)} rows)")

        completed.add(name)
        save_state("datagovsg", {"completed": list(completed)})

        # Rate limit: 0.5s between datasets
        if i < len(pending):
            time.sleep(0.5)

    print(f"  Completed: {len(completed)}/{len(DATASETS)} datasets")
