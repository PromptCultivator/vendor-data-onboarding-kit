# Component: Vendor Manifest Model (`vdok.manifest`)

## 1. Overview & Responsibility

The vendor manifest is the declarative specification that configures how raw vendor drops are interpreted, parsed, validated, and normalized. Every onboarded vendor requires a declarative configuration file placed in `manifests/<vendor>.yaml`.

The `vdok.manifest` module parses, validates, and serializes these files into strongly typed Pydantic models. It guarantees that all subsequent pipeline stages (discovery, normalization, validation, and storage) operate against verified configuration contracts.

---

## 2. Specification & Data Structure

### 2.1 Configuration Schema

```yaml
vendor: sec_fsds                          # Unique alphanumeric vendor identifier
entity_columns: [cik]                     # Columns that identify the entity, in priority order
entity_id_type: cik                       # cik | ticker | isin | cusip | vendor_id
event_time_column: ddate                  # Source column denoting the observation timestamp
event_time_format: "%Y%m%d"               # strptime format string or ISO-8601 specifier
knowledge_time:
  source: column                          # column | drop_time
  column: filed                           # Required when source is column
  format: "%Y%m%d"                        # Parsing format when source is column
timezone: UTC                             # Source timezone for naive timestamps
value_columns: [value]                    # Columns containing quantitative or categorical facts
unit_column: uom                          # Optional column naming the measurement unit
field_columns: [tag]                      # Columns combined to form the canonical field name
expected_calendar: quarterly              # daily | weekly | monthly | quarterly | none
expected_universe: null                   # Optional path to a file containing expected entity IDs
rules: rules/sec_fsds.yaml                # Relative path to compiled validation rules
```

---

## 3. Pydantic Model Architecture

The implementation uses Pydantic v2 to validate configuration types and enforce invariants:

```python
from enum import Enum
from pathlib import Path
from pydantic import BaseModel, Field, model_validator


class EntityIdType(str, Enum):
    CIK = "cik"
    TICKER = "ticker"
    ISIN = "isin"
    CUSIP = "cusip"
    VENDOR_ID = "vendor_id"


class ExpectedCalendar(str, Enum):
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    QUARTERLY = "quarterly"
    NONE = "none"


class KnowledgeTimeSource(str, Enum):
    COLUMN = "column"
    DROP_TIME = "drop_time"


class KnowledgeTimeConfig(BaseModel):
    source: KnowledgeTimeSource
    column: str | None = None
    format: str | None = None

    @model_validator(mode="after")
    def verify_column_requirements(self) -> "KnowledgeTimeConfig":
        if self.source == KnowledgeTimeSource.COLUMN and not self.column:
            raise ValueError("Knowledge time source 'column' requires 'column' name")
        return self


class VendorManifest(BaseModel):
    vendor: str = Field(..., pattern=r"^[a-z0-9_-]+$")
    entity_columns: list[str] = Field(..., min_length=1)
    entity_id_type: EntityIdType
    event_time_column: str
    event_time_format: str = "%Y-%m-%d"
    knowledge_time: KnowledgeTimeConfig
    timezone: str = "UTC"
    value_columns: list[str] = Field(..., min_length=1)
    unit_column: str | None = None
    field_columns: list[str] = Field(default_factory=list)
    expected_calendar: ExpectedCalendar = ExpectedCalendar.NONE
    expected_universe: Path | None = None
    rules: Path | None = None
```

---

## 4. Operational Invariants

1. **Temporal Definition**: If `knowledge_time.source` is `drop_time`, the system sets knowledge time to the arrival timestamp recorded at drop ingestion. If `column` is specified, the source file must provide valid date or datetime values.
2. **Deterministic Precedence**: `entity_columns` defines a priority hierarchy. The normalizer evaluates columns from left to right, selecting the first non-null identifier present in the record.
3. **Calendar Expectation**: `expected_calendar` governs gap detection within the readiness report. If a quarterly dataset lacks observations for an entire calendar quarter, validation marks the absence as an anomaly.

---

## 5. Error Modes & Edge Cases

| Failure Mode | Detection Mechanism | System Action |
|---|---|---|
| Manifest File Missing | File system existence check | Halts CLI execution with non-zero exit code |
| Invalid YAML Syntax | YAML parser exception | Emits syntax location error; aborts pipeline |
| Unknown Identifier Type | Pydantic enum validation | Rejects configuration; displays valid types |
| Missing Knowledge Column | Model cross-field validator | Rejects configuration before file processing |
| Unknown Timezone Identifier | `zoneinfo` lookup validation | Rejects configuration before parsing timestamps |
