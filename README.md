# MAS Singapore Data

**Source:** Monetary Authority of Singapore via [data.gov.sg](https://data.gov.sg/)
**License:** [Singapore Open Data Licence](https://data.gov.sg/open-data-licence)

## Datasets (16)

### Exchange Rates (4)
| Dataset | Frequency | Format | Description |
|---------|-----------|--------|-------------|
| `mas_exchange_rates_usd_daily` | Daily | Long | SGD/USD rate from 1988 |
| `mas_exchange_rates_usd_annual` | Annual | Long | SGD/USD annual average |
| `mas_exchange_rates_avg_monthly` | Monthly | Wide→Long | 15 currency pairs (USD, GBP, EUR, JPY, etc.) |
| `mas_exchange_rates_avg_annual` | Annual | Wide→Long | 14 currency pairs annual averages |

### Interest Rates (1)
| Dataset | Frequency | Format | Description |
|---------|-----------|--------|-------------|
| `mas_bank_interest_rates` | Monthly | Wide→Long | Govt securities yields, SORA, compounded SORA |

### Money & Currency (3)
| Dataset | Frequency | Format | Description |
|---------|-----------|--------|-------------|
| `mas_money_supply` | Monthly | Wide→Long | M1/M2/M3 and components (current series) |
| `mas_money_supply_historical` | Monthly | Wide→Long | M1/M2/M3 historical (includes POSB) |
| `mas_currency_in_circulation` | Monthly | Wide→Long | Notes and coins in circulation |

### Banking (5)
| Dataset | Frequency | Format | Description |
|---------|-----------|--------|-------------|
| `mas_commercial_bank_loans` | Monthly | Wide→Long | Loans by 27 industry/consumer categories |
| `mas_commercial_bank_loans_quarterly` | Quarterly | Wide→Long | Housing and bridging loans |
| `mas_loans_by_sector` | Monthly | Long | Loans to non-bank customers by sector |
| `mas_finance_company_loans` | Monthly | Wide→Long | Hire purchase and housing loans |
| `mas_credit_charge_cards` | Annual | Wide→Long | Cardholders, billings, charge-off rates |

### Government & Markets (3)
| Dataset | Frequency | Format | Description |
|---------|-----------|--------|-------------|
| `mas_fx_market_turnover` | Monthly | Wide→Long | FX market total and daily average turnover |
| `mas_govt_debt_by_maturity` | Annual | Wide→Long | Domestic and external debt by maturity |
| `mas_govt_debt_by_instrument` | Annual | Wide→Long | SGS, T-bills, savings bonds, external debt |

## Coverage Decisions

**Included:** All MAS datasets available via the data.gov.sg API (16 datasets).

**Excluded:**
- `exchange_rates_avg_monthly_alt` — Duplicate of `exchange_rates_avg_monthly` with shorter time range (ends ~1 year earlier). Same 15 currencies.
- MAS Monthly Statistical Bulletin HTML tables — Available only as ASP.NET-rendered HTML. Fragile to scrape, and the key data (interest rates, exchange rates, money supply, loans) is already covered by the API datasets.
- MAS eservices exchange rates / interest rates pages — Single HTML pages requiring form submission. Data already available via API.

## Data Format

Most datasets come from the API in wide format (one column per time period) and are unpivoted to long format with three columns:

| Column | Description |
|--------|-------------|
| `data_series` | Name of the metric or series |
| `period` | Time period: `YYYY-MM` (monthly), `YYYY-QN` (quarterly), `YYYY` (annual) |
| `value` | Numeric value (null where original data shows "na") |

Three datasets are already in long format and keep their original column structure.
