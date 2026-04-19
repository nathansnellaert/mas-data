"""MAS Data connector utilities.

Shared logic for fetching and transforming data.gov.sg datasets.
"""
import re
import time
import pyarrow as pa
from subsets_utils import get

BASE_URL = "https://api-production.data.gov.sg/v2/public/api/datasets"

MONTH_ABBR = {
    "Jan": "01", "Feb": "02", "Mar": "03", "Apr": "04",
    "May": "05", "Jun": "06", "Jul": "07", "Aug": "08",
    "Sep": "09", "Oct": "10", "Nov": "11", "Dec": "12",
}


def fetch_rows(dataset_id):
    """Fetch all rows from a data.gov.sg dataset, handling pagination."""
    rows = []
    url = f"{BASE_URL}/{dataset_id}/list-rows?limit=5000"

    while url:
        response = get(url, timeout=60.0)
        response.raise_for_status()
        data = response.json()

        batch = data.get("data", {}).get("rows", [])
        rows.extend(batch)

        next_cursor = data.get("data", {}).get("links", {}).get("next")
        if next_cursor and batch:
            url = f"{BASE_URL}/{dataset_id}/list-rows?limit=5000&{next_cursor}"
        else:
            break

        time.sleep(0.2)

    return rows


def parse_period(col_name):
    """Parse a time-period column name to a normalized date string.

    Returns (period, frequency) or (None, None) if not a period column.
    '2025Oct' → ('2025-10', 'monthly')
    '20253Q'  → ('2025-Q3', 'quarterly')
    '2024'    → ('2024', 'annual')
    """
    m = re.match(r"^(\d{4})(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)$", col_name)
    if m:
        return f"{m.group(1)}-{MONTH_ABBR[m.group(2)]}", "monthly"

    m = re.match(r"^(\d{4})(\d)Q$", col_name)
    if m:
        return f"{m.group(1)}-Q{m.group(2)}", "quarterly"

    m = re.match(r"^(\d{4})$", col_name)
    if m:
        return col_name, "annual"

    return None, None


def parse_value(raw):
    """Parse a raw API value to float, converting 'na' and blanks to None."""
    if raw is None:
        return None
    s = str(raw).strip()
    if s.lower() in ("na", "n.a.", "-", ""):
        return None
    try:
        return float(s.replace(",", ""))
    except ValueError:
        return None


def unpivot_wide(rows):
    """Convert wide-format rows to a long-format PyArrow table.

    Input rows have a 'DataSeries' label column plus time-period columns.
    Output: table with columns (data_series, period, value).
    """
    if not rows:
        return pa.table({
            "data_series": pa.array([], type=pa.string()),
            "period": pa.array([], type=pa.string()),
            "value": pa.array([], type=pa.float64()),
        })

    period_cols = []
    for key in rows[0]:
        period, _ = parse_period(key)
        if period:
            period_cols.append((key, period))

    period_cols.sort(key=lambda x: x[1])

    out_series = []
    out_period = []
    out_value = []

    for row in rows:
        series_name = row.get("DataSeries", "").strip()
        for col, period in period_cols:
            out_series.append(series_name)
            out_period.append(period)
            out_value.append(parse_value(row.get(col)))

    return pa.table({
        "data_series": pa.array(out_series, type=pa.string()),
        "period": pa.array(out_period, type=pa.string()),
        "value": pa.array(out_value, type=pa.float64()),
    })


def rows_to_table(rows, schema):
    """Convert JSON rows to a PyArrow table with the given schema.

    Handles 'na'/blank → null, string→float conversion for numeric columns.
    Drops columns not in the schema (e.g. vault_id).
    """
    arrays = {}
    for field in schema:
        values = []
        for row in rows:
            raw = row.get(field.name)
            if pa.types.is_floating(field.type):
                values.append(parse_value(raw))
            else:
                if raw is None:
                    values.append(None)
                else:
                    s = str(raw).strip()
                    values.append(None if s.lower() in ("na", "") else s)
        arrays[field.name] = pa.array(values, type=field.type)

    return pa.table(arrays)
