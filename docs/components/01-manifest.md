# Component: Vendor Manifest Model (`vdok.manifest`)

## 1. Overview & Responsibility

`vdok.manifest` parses, validates, and serializes declarative vendor configurations stored in `manifests/<vendor>.yaml`. Every downstream pipeline stage—discovery, schema checking, normalization, validation, storage, and reporting—consumes a validated `VendorManifest` instance.

---

## 2. Configuration Schema

```yaml
vendor: sec_fsds
entity_columns: [cik]
entity_id_type: cik
event_time_column: ddate
event_time_format: "%Y%m%d"
knowledge_time:
  source: column
  column: filed
  format: "%Y%m%d"
timezone: UTC
value_columns: [value]
unit_column: uom
field_columns: [tag]
expected_calendar: quarterly
expected_universe: null
rules: rules/sec_fsds.yaml
```

---

## 3. Pydantic v2 Data Model

`vdok/manifest.py` defines strongly typed enumerations and validators:

- **`EntityIdType`**: `cik`, `ticker`, `isin`, `cusip`, `vendor_id`.
- **`ExpectedCalendar`**: `daily`, `weekly`, `monthly`, `quarterly`, `none`.
- **`KnowledgeTimeSource`**: `column`, `drop_time`.
- **`KnowledgeTimeConfig`**: Validates via `model_validator(mode="after")` that `column` is provided whenever `source == "column"`.
- **`VendorManifest`**:
  - Enforces `vendor` pattern `^[a-z0-9_-]+$`, non-empty `entity_columns` (in fallback priority order), and non-empty `value_columns`.
  - Validates `timezone` against standard-library `zoneinfo.ZoneInfo`.
  - Exposes `from_yaml`, `from_file`, `to_yaml`, and `to_file`.

---

## 4. Operational Invariants

1. **Knowledge Timestamp Source**: When `knowledge_time.source` is `drop_time`, normalization applies a single drop-level UTC receipt timestamp (or CLI `--as-of` override). When `source` is `column`, timestamps are parsed from the named source column.
2. **Entity Column Precedence**: `entity_columns` lists candidate identifier columns from left to right; normalization selects the first non-null identifier per row.
3. **Calendar Expectations**: `expected_calendar` drives period aggregation and missing-period gap detection in validation (`coverage_gap`) and readiness reporting (`compute_coverage`).

---

## 5. Error Modes

| Failure Mode | Detection Mechanism | System Behavior |
|---|---|---|
| Missing manifest file | `VendorManifest.from_file` existence check | Raises `FileNotFoundError`; CLI exits with code `2` |
| Non-mapping / invalid YAML | `yaml.safe_load` type check | Raises `ValueError` |
| Unknown identifier or calendar enum | Pydantic `Enum` validation | Raises `ValidationError` |
| Missing `knowledge_time.column` | `KnowledgeTimeConfig` model validator | Raises `ValueError` before ingestion |
| Invalid IANA timezone | `zoneinfo.ZoneInfo(v)` lookup | Raises `ValueError` |
