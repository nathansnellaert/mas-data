"""Ingest MAS domestic interest rates data.

The MAS provides interest rates including SORA (Singapore Overnight Rate Average),
SIBOR, and other key rates through an HTML page.

Source: https://eservices.mas.gov.sg/statistics/dir/domesticinterestrates.aspx
"""

from subsets_utils import get, save_raw_file, load_state, save_state


BASE_URL = "https://eservices.mas.gov.sg/statistics/dir/domesticinterestrates.aspx"


def run():
    """Fetch domestic interest rates page (contains embedded data)."""
    state = load_state("interest_rates")

    if state.get("fetched"):
        print("  Interest rates page already fetched")
        return

    print("  Fetching domestic interest rates page...")

    response = get(BASE_URL, timeout=60.0)
    response.raise_for_status()

    save_raw_file(response.text, "interest_rates_page", extension="html")

    save_state("interest_rates", {"fetched": True})
    print("    -> saved interest_rates_page.html")
