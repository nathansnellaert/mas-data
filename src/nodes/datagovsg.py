"""MAS datasets from data.gov.sg API.

Covers exchange rates, interest rates, money supply, banking statistics,
and government debt from the Monetary Authority of Singapore.

Source: https://data.gov.sg/
API docs: https://guide.data.gov.sg/developer-guide/dataset-apis
License: Singapore Open Data Licence (https://data.gov.sg/open-data-licence)
"""
import time
import pyarrow as pa
from connector_utils import fetch_rows, unpivot_wide, rows_to_table
from subsets_utils import (
    save_raw_json, load_raw_json,
    merge, publish,
    load_state, save_state, data_hash,
)

LICENSE = "Singapore Open Data Licence (https://data.gov.sg/open-data-licence)"

WIDE_COLUMN_DESCRIPTIONS = {
    "data_series": "Name of the data series or metric",
    "period": "Time period (YYYY-MM for monthly, YYYY-QN for quarterly, YYYY for annual)",
    "value": "Numeric value for the series in the given period",
}

# ── Dataset configurations ────────────────────────────────────────────

WIDE_DATASETS = {
    "mas_bank_interest_rates": {
        "api_id": "d_5fe5a4bb4a1ecc4d8a56a095832e2b24",
        "metadata": {
            "title": "MAS Bank Interest Rates (Monthly)",
            "description": "Key Singapore interest rates including government securities yields (1-20 year), SORA, and compounded SORA. End-of-period monthly rates in percent per annum.",
        },
    },
    "mas_money_supply": {
        "api_id": "d_7ed3eccba609ac0bdfcf406d939bdb0b",
        "metadata": {
            "title": "MAS Money Supply (Monthly)",
            "description": "Singapore money supply aggregates (M1, M2, M3) and components: currency in active circulation, demand deposits, quasi-money (fixed deposits, NCDs, savings), and net deposits with finance companies. Monthly, in million SGD.",
        },
    },
    "mas_money_supply_historical": {
        "api_id": "d_4c6bd8b2c4aa7041a31f3ed0cd122c47",
        "metadata": {
            "title": "MAS Money Supply Historical (Monthly)",
            "description": "Historical Singapore money supply aggregates (M1, M2, M3) and components, including POSB deposits. Covers earlier periods not in the current series. Monthly, in million SGD.",
        },
    },
    "mas_currency_in_circulation": {
        "api_id": "d_10036483fced016b239ce7d2ab175125",
        "metadata": {
            "title": "MAS Currency in Circulation (Monthly)",
            "description": "Gross currency in circulation in Singapore, broken down by notes and coins. Monthly, in million SGD.",
        },
    },
    "mas_commercial_bank_loans": {
        "api_id": "d_af0415517a3a3a94b3b74039934ef976",
        "metadata": {
            "title": "MAS Commercial Bank Loans (Monthly)",
            "description": "Commercial bank loans and advances in Singapore by industry sector and consumer loan type. Includes business loans (agriculture, manufacturing, construction, commerce, transport, financial services) and consumer loans (housing, car, credit card, share financing). Monthly, in million SGD.",
        },
    },
    "mas_commercial_bank_loans_quarterly": {
        "api_id": "d_0396bc943075a37d44c720ceb5be660a",
        "metadata": {
            "title": "MAS Commercial Bank Loans (Quarterly)",
            "description": "Commercial bank consumer housing and bridging loans in Singapore. Quarterly, in million SGD.",
        },
    },
    "mas_finance_company_loans": {
        "api_id": "d_4f73f4471a84f944ed37b651a8227ad8",
        "metadata": {
            "title": "MAS Finance Company Loans (Monthly)",
            "description": "Finance company loans and advances in Singapore, broken down by hire purchase (motor vehicles, consumer durables, other goods) and housing loans. Monthly, in million SGD.",
        },
    },
    "mas_fx_market_turnover": {
        "api_id": "d_6dd6162d59737d67edfb35026dfd58c2",
        "metadata": {
            "title": "MAS FX Market Turnover (Monthly)",
            "description": "Singapore foreign exchange market turnover: total and daily average, in both SGD millions and USD millions. Monthly.",
        },
    },
    "mas_govt_debt_by_maturity": {
        "api_id": "d_fd4b8728cb059c04fc0322199f4b2696",
        "metadata": {
            "title": "MAS Government Debt by Maturity (Annual)",
            "description": "Singapore government debt by maturity: domestic debt (excluding advance deposits) split by 1-year-or-less and more-than-1-year maturity, plus government external debt by maturity. Annual, in million SGD.",
        },
    },
    "mas_govt_debt_by_instrument": {
        "api_id": "d_d4f7c9d15692b3c08aa9bc8bc56c0a72",
        "metadata": {
            "title": "MAS Government Debt by Instrument (Annual)",
            "description": "Singapore government debt by instrument type: domestic debt (SGS, T-bills, savings bonds, special SGS, RMGS) and external debt (World Bank, ADB, others), plus debt guarantees. Annual, in million SGD.",
        },
    },
    "mas_credit_charge_cards": {
        "api_id": "d_b40deadbdc470e97b9e16de99c5e6ee2",
        "metadata": {
            "title": "MAS Credit and Charge Cards (Annual)",
            "description": "Singapore credit and charge card statistics: principal and supplementary cardholders, total billings, rollover balance, bad debts written off, and charge-off rates. Annual.",
        },
    },
    "mas_exchange_rates_avg_monthly": {
        "api_id": "d_b2b7ffe00aaec3936ed379369fdf531b",
        "metadata": {
            "title": "MAS Exchange Rates Average (Monthly)",
            "description": "Monthly average exchange rates for 15 currencies against the Singapore dollar: USD, GBP, CHF, JPY, MYR, HKD, AUD, KRW, TWD, EUR, IDR, THB, CNY, INR, PHP. Rates expressed as SGD per unit of foreign currency.",
        },
    },
    "mas_exchange_rates_avg_annual": {
        "api_id": "d_b09aeaf8eb591c4bfe347b66148c6b53",
        "metadata": {
            "title": "MAS Exchange Rates Average (Annual)",
            "description": "Annual average exchange rates for 14 currencies against the Singapore dollar. Rates expressed as SGD per unit of foreign currency.",
        },
    },
}

LONG_DATASETS = {
    "mas_exchange_rates_usd_daily": {
        "api_id": "d_046ff8d521a218d9178178cfbfc45c2c",
        "schema": pa.schema([
            ("date", pa.string()),
            ("exchange_rate_usd", pa.float64()),
        ]),
        "key": ["date"],
        "metadata": {
            "title": "MAS SGD/USD Exchange Rate (Daily)",
            "description": "Daily SGD per USD exchange rate from the Monetary Authority of Singapore. Covers trading days from 1988 to present.",
            "column_descriptions": {
                "date": "Trading date (YYYY-MM-DD)",
                "exchange_rate_usd": "SGD per 1 USD",
            },
        },
    },
    "mas_exchange_rates_usd_annual": {
        "api_id": "d_6cb7c12d5f25f0a04e70657dfebcb514",
        "schema": pa.schema([
            ("year", pa.string()),
            ("sgd_per_unit_of_usd", pa.float64()),
        ]),
        "key": ["year"],
        "metadata": {
            "title": "MAS SGD/USD Exchange Rate (Annual)",
            "description": "Annual average SGD per USD exchange rate from the Monetary Authority of Singapore.",
            "column_descriptions": {
                "year": "Calendar year (YYYY)",
                "sgd_per_unit_of_usd": "Annual average SGD per 1 USD",
            },
        },
    },
    "mas_loans_by_sector": {
        "api_id": "d_c2e116320c9d36f6ea6cdd82fb763de2",
        "schema": pa.schema([
            ("month", pa.string()),
            ("sector", pa.string()),
            ("subsector", pa.string()),
            ("amount", pa.float64()),
        ]),
        "key": ["month", "sector", "subsector"],
        "column_rename": {"level_1": "sector", "level_2": "subsector", "total_loans": "amount"},
        "metadata": {
            "title": "MAS Total Loans to Non-Bank Customers by Sector (Monthly)",
            "description": "Total loans and advances to non-bank customers in Singapore by economic sector and subsector. Monthly, in million SGD.",
            "column_descriptions": {
                "month": "Month of observation (YYYY-MM)",
                "sector": "Economic sector (Business, Consumer)",
                "subsector": "Industry subsector",
                "amount": "Total loans in million SGD",
            },
        },
    },
}

ALL_DATASETS = {**WIDE_DATASETS, **LONG_DATASETS}


# ── Download ──────────────────────────────────────────────────────────

def download():
    """Fetch all MAS datasets from data.gov.sg API."""
    print("Fetching MAS datasets from data.gov.sg...")

    state = load_state("datagovsg_download")
    completed = set(state.get("completed", []))

    pending = [(name, cfg) for name, cfg in ALL_DATASETS.items() if name not in completed]

    if not pending:
        print("  All datasets already downloaded")
        return

    print(f"  {len(pending)} datasets to fetch...")

    for i, (name, cfg) in enumerate(pending, 1):
        print(f"  [{i}/{len(pending)}] {name}")

        rows = fetch_rows(cfg["api_id"])
        save_raw_json(rows, name)
        print(f"    -> {len(rows)} rows")

        completed.add(name)
        save_state("datagovsg_download", {"completed": list(completed)})

        if i < len(pending):
            time.sleep(0.3)

    print(f"  Download complete: {len(ALL_DATASETS)} datasets")


# ── Transform ─────────────────────────────────────────────────────────

def transform():
    """Transform all downloaded datasets and publish."""
    print("Transforming MAS datasets...")

    for name, cfg in WIDE_DATASETS.items():
        _transform_wide(name, cfg)

    for name, cfg in LONG_DATASETS.items():
        _transform_long(name, cfg)

    print("  Transform complete")


def _load_rows(name):
    """Load raw JSON rows, handling both old and new save formats."""
    data = load_raw_json(name)
    if isinstance(data, dict) and "rows" in data:
        return data["rows"]
    return data


def _transform_wide(name, cfg):
    rows = _load_rows(name)
    table = unpivot_wide(rows)

    h = data_hash(table)
    if load_state(name).get("hash") == h:
        print(f"  {name}: unchanged, skipping")
        return

    print(f"  {name}: {table.num_rows} rows")

    merge(table, name, key=["data_series", "period"])

    publish(name, {
        "id": name,
        "title": cfg["metadata"]["title"],
        "description": cfg["metadata"]["description"],
        "license": LICENSE,
        "column_descriptions": WIDE_COLUMN_DESCRIPTIONS,
    })
    save_state(name, {"hash": h})


def _transform_long(name, cfg):
    rows = _load_rows(name)

    rename = cfg.get("column_rename", {})
    if rename:
        rows = [{rename.get(k, k): v for k, v in row.items()} for row in rows]

    table = rows_to_table(rows, cfg["schema"])

    h = data_hash(table)
    if load_state(name).get("hash") == h:
        print(f"  {name}: unchanged, skipping")
        return

    print(f"  {name}: {table.num_rows} rows")

    merge(table, name, key=cfg["key"])

    publish(name, {
        "id": name,
        "title": cfg["metadata"]["title"],
        "description": cfg["metadata"]["description"],
        "license": LICENSE,
        "column_descriptions": cfg["metadata"]["column_descriptions"],
    })
    save_state(name, {"hash": h})


NODES = {
    download: [],
    transform: [download],
}

if __name__ == "__main__":
    download()
    transform()
