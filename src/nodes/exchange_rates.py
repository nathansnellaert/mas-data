"""Ingest MAS exchange rates data.

The MAS provides exchange rates through an HTML page with data table.
We fetch all available currency pairs with historical data.

Source: https://eservices.mas.gov.sg/statistics/msb/exchangerates.aspx
"""

from subsets_utils import get, save_raw_file, load_state, save_state
import re

BASE_URL = "https://eservices.mas.gov.sg/statistics/msb/exchangerates.aspx"


def extract_viewstate(html):
    """Extract ASP.NET ViewState from page for form submissions."""
    patterns = {
        '__VIEWSTATE': r'id="__VIEWSTATE"[^>]*value="([^"]*)"',
        '__VIEWSTATEGENERATOR': r'id="__VIEWSTATEGENERATOR"[^>]*value="([^"]*)"',
        '__EVENTVALIDATION': r'id="__EVENTVALIDATION"[^>]*value="([^"]*)"',
    }
    result = {}
    for key, pattern in patterns.items():
        match = re.search(pattern, html)
        if match:
            result[key] = match.group(1)
    return result


def run():
    """Fetch exchange rates page (contains embedded data table)."""
    print("Fetching MAS exchange rates...")

    state = load_state("exchange_rates")

    if state.get("fetched"):
        print("  Exchange rates page already fetched")
        return

    print("  Fetching exchange rates page...")

    response = get(BASE_URL, timeout=60.0)
    response.raise_for_status()

    save_raw_file(response.text, "exchange_rates_page", extension="html")

    save_state("exchange_rates", {"fetched": True})
    print("    -> saved exchange_rates_page.html")


NODES = {
    run: [],
}

if __name__ == "__main__":
    run()
