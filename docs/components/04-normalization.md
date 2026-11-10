# Component: Canonical Normalization (`vdok.normalize`)

## 1. Overview & Responsibility

The `vdok.normalize` module transforms diverse raw vendor records into a standardized canonical row format. Vendor data arrives with heterogeneous conventions: divergent date representations, unstandardized units of measure, whitespace irregularities, and variable numeric formats.

The normalization layer isolates downstream components from vendor-specific variances by executing pure, deterministic transformations. Every normalization function is tested with property-based testing (Hypothesis) to verify invariant preservation across arbitrary inputs.

---

## 2. Canonical Row Specification

Every record in the storage engine conforms to the Apache Arrow schema defined below:

```
entity_id:       string                  # Resolved canonical entity identifier
entity_id_type:  string                  # Identifier classification (e.g., "cik", "ticker")
field:           string                  # Joined field name (e.g., "Revenues|USD|4Q")
event_time:      timestamp[us, UTC]      # Microsecond timestamp in UTC
knowledge_time:  timestamp[us, UTC]      # Microsecond timestamp in UTC
value_num:       decimal128(38, 9)       # Fixed-precision decimal value (null if categorical)
value_str:       string                  # Text value (null if numeric)
unit:            string                  # Canonical unit of measure (e.g., "USD", "shares")
drop_id:         string                  # 16-character source drop identifier
source_file:     string                  # Relative file path of the origin record
source_row:      int64                   # 1-based index within the origin file
row_hash:        string                  # SHA-256 fingerprint of normalized values
```

---

## 3. Pure Normalization Functions

### 3.1 Decimal Monetary Parsing
Floating-point numbers can introduce representation errors (such as `$19.99` becoming `19.989999999999998`). The normalizer parses numbers directly from string representations into exact fixed-point decimals:

```python
from decimal import Decimal, InvalidOperation


def parse_decimal_exact(raw_val: str | int | float | None) -> Decimal | None:
    if raw_val is None or raw_val == "":
        return None
    if isinstance(raw_val, float):
        raise TypeError(
            f"Binary floating-point value {raw_val} rejected for monetary field; "
            "exact string or integer input required."
        )
    try:
        return Decimal(str(raw_val).strip())
    except InvalidOperation as err:
        raise ValueError(f"Malformed decimal literal: {raw_val}") from err
```

### 3.2 Temporal Standardization to UTC
Source dates and datetimes are parsed using format specifiers from the vendor manifest. Naive timestamps have their source timezone attached and are then converted to UTC:

```python
from datetime import datetime, date, timezone
from zoneinfo import ZoneInfo


def parse_timestamp_utc(raw_val: str, fmt: str, source_tz: str = "UTC") -> datetime:
    parsed = datetime.strptime(raw_val.strip(), fmt)
    if parsed.tzinfo is None:
        tz = ZoneInfo(source_tz)
        localized = parsed.replace(tzinfo=tz)
    else:
        localized = parsed
    return localized.astimezone(timezone.utc)
```

### 3.3 Unit Standardization
Measurement units are mapped against an immutable reference table:

| Input Variant | Canonical Unit |
|---|---|
| `USD`, `usd`, `US$`, `dollars` | `USD` |
| `shares`, `Shares`, `SHARES` | `shares` |
| `pure`, `scalar`, `dimensionless` | `pure` |
| `%`, `percent`, `pct` | `ratio` (scaled: 100% = 1.0) |

### 3.4 Identifier Folding
String identifiers are stripped of leading and trailing whitespace, and alphanumeric codes (such as ticker symbols) are converted to uppercase.

---

## 4. Cryptographic Row Fingerprint (`row_hash`)

To verify row uniqueness and detect silent data modifications, each record receives a SHA-256 fingerprint:

```
row_hash = SHA256(entity_id || field || event_time || value_num || value_str || unit)
```

This hash is calculated after canonicalization, providing an invariant signature that is independent of serialization formatting.
