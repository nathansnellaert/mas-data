import argparse

from subsets_utils import validate_environment
from ingest import msb_tables, exchange_rates, interest_rates, datagovsg


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ingest-only", action="store_true", help="Only fetch data from MAS sources")
    args = parser.parse_args()

    validate_environment()

    print("\n=== Phase 1: Ingest ===")

    # Fetch from data.gov.sg (structured API data)
    print("\n--- Data.gov.sg datasets ---")
    datagovsg.run()

    # Fetch Monthly Statistical Bulletin tables
    print("\n--- Monthly Statistical Bulletin tables ---")
    msb_tables.run()

    # Fetch exchange rates page
    print("\n--- Exchange rates ---")
    exchange_rates.run()

    # Fetch interest rates page
    print("\n--- Interest rates ---")
    interest_rates.run()

    print("\n=== MAS connector complete ===")


if __name__ == "__main__":
    main()
