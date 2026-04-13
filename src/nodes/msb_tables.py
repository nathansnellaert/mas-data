"""Ingest MAS Monthly Statistical Bulletin (MSB) tables.

The MAS provides historical monthly statistics via ASP.NET pages.
Data is available as HTML tables which we scrape since the API is in maintenance.

Source: https://eservices.mas.gov.sg/statistics/msb-xml/msb-statistics-history/
"""

from subsets_utils import get, save_raw_file, load_state, save_state
import time

TABLES = {
    # === Section I: Money and Banking ===
    "I.1": ("I.H", "Money Supply (DBU)"),
    "I.1A": ("I.H", "Money Supply (DBU and ACU)"),
    "I.2A": ("I.H", "Monetary Survey (DBU)"),
    "I.2B": ("I.H", "Monetary Survey (DBU and ACU)"),
    "I.3A": ("I.H", "Commercial Banks: Assets and Liabilities of DBUs"),
    "I.3B": ("I.H", "Commercial Banks: Assets of DBUs"),
    "I.3C": ("I.H", "Commercial Banks: Liabilities of DBUs"),
    "I.4": ("I.H", "Commercial Banks: Deposits by Types of Non-bank Customers"),
    "I.5A": ("I.H", "Commercial Banks: Loans to Non-Bank Customers by Industry (DBU)"),
    "I.5B": ("I.H", "Commercial Banks: Loans to Non-Bank Customers by Industry (ACU)"),
    "I.5C": ("I.H", "Commercial Banks: Loans to SPV for Covered Bond Issuances"),
    "I.6": ("I.H", "Commercial Banks: Loan Limits by Industry"),
    "I.7": ("I.H", "Commercial Banks: Types of Loans to Non-Bank Customers"),
    "I.8": ("I.H", "Commercial Banks: Statutory Liquidity Position of DBUs"),
    "I.9": ("I.H", "Commercial Banks: Maturities of Assets and Liabilities"),
    "I.10": ("I.H", "Commercial Banks: External Assets and Liabilities (DBU)"),
    "I.10A": ("I.H", "Commercial Banks: External Assets and Liabilities (DBU and ACU)"),
    "I.11": ("I.H", "Commercial Banks: Combined Assets and Liabilities"),
    "I.12": ("I.H", "Commercial Banks: Classified Exposures"),
    "I.12A": ("I.H", "Commercial Banks: Non-Performing Loans by Sector"),
    "I.13": ("I.H", "Asian Dollar Market: Assets of ACUs"),
    "I.14": ("I.H", "Asian Dollar Market: Liabilities of ACUs"),
    "I.15": ("I.H", "Asian Dollar Market: Maturities of Assets and Liabilities"),
    "I.16": ("I.H", "Asian Dollar Market: Interbank and Non-Bank Funds by Region"),
    "I.18": ("I.H", "Commercial Banks: Non-Bank Loan to Deposit Ratios"),

    # === Section II: Non-Bank Financial Institutions ===
    "II.3": ("II.H", "Merchant Banks: Assets and Liabilities (Domestic and ACU)"),
    "II.4": ("II.H", "Merchant Banks: Assets and Liabilities (Domestic)"),

    # === Section III: Financial Markets ===
    "III.2": ("III.H", "Foreign Exchange Market Turnover"),
    "III.4": ("III.H", "SGS: Issuance, Redemption and Outstanding"),
}

BASE_URL = "https://eservices.mas.gov.sg/statistics/msb-xml/msb-statistics-history/Report.aspx"


def run():
    """Fetch all MSB tables from MAS and save as HTML."""
    print("Fetching MAS Monthly Statistical Bulletin tables...")

    state = load_state("msb_tables")
    completed = set(state.get("completed", []))

    pending = [(table_id, info) for table_id, info in TABLES.items() if table_id not in completed]

    if not pending:
        print("  All MSB tables up to date")
        return

    print(f"  Fetching {len(pending)} MSB tables...")

    for i, (table_id, (table_set_id, description)) in enumerate(pending, 1):
        print(f"  [{i}/{len(pending)}] Fetching {table_id} ({description})...")

        url = f"{BASE_URL}?tableSetID={table_set_id}&tableID={table_id}"

        response = get(url, timeout=60.0)
        response.raise_for_status()

        filename = f"msb_{table_id.replace('.', '_')}"
        save_raw_file(response.text, filename, extension="html")

        completed.add(table_id)
        save_state("msb_tables", {"completed": list(completed)})

        print(f"    -> saved {filename}.html")

        time.sleep(0.5)

    print(f"  Completed: {len(completed)}/{len(TABLES)} tables")


NODES = {
    run: [],
}

if __name__ == "__main__":
    run()
