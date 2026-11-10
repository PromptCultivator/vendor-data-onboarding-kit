"""Vendor manifest data models and validation.

Defines Pydantic models for vendor manifest configuration files (manifests/<vendor>.yaml),
enforcing taxonomy constraints, IANA timezone validation, and YAML serialization.
"""

from __future__ import annotations

import zoneinfo
from enum import Enum
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator


class EntityIdType(str, Enum):
    """Supported entity identifier taxonomy classifications."""

    CIK = "cik"
    TICKER = "ticker"
    ISIN = "isin"
    CUSIP = "cusip"
    VENDOR_ID = "vendor_id"


class ExpectedCalendar(str, Enum):
    """Expected calendar reporting cadences."""

    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    QUARTERLY = "quarterly"
    NONE = "none"


class KnowledgeTimeSource(str, Enum):
    """Source mode for determining knowledge timestamp."""

    COLUMN = "column"
    DROP_TIME = "drop_time"


class KnowledgeTimeConfig(BaseModel):
    """Configuration for extracting knowledge timestamp from drop or metadata."""

    source: KnowledgeTimeSource
    column: str | None = None
    format: str | None = None

    @model_validator(mode="after")
    def verify_column_requirements(self) -> KnowledgeTimeConfig:
        if self.source == KnowledgeTimeSource.COLUMN and not self.column:
            raise ValueError(
                "Knowledge time source 'column' requires 'column' name to be specified."
            )
        return self


class VendorManifest(BaseModel):
    """Complete declarative configuration for onboarding a vendor dataset."""

    vendor: str = Field(
        ...,
        pattern=r"^[a-z0-9_-]+$",
        description="Unique alphanumeric vendor code.",
    )
    entity_columns: list[str] = Field(
        ...,
        min_length=1,
        description="Entity identifier columns in priority order.",
    )
    entity_id_type: EntityIdType = Field(
        ...,
        description="Classification of primary entity identifier.",
    )
    event_time_column: str = Field(
        ...,
        description="Source column containing observation or period time.",
    )
    event_time_format: str = Field(
        default="%Y-%m-%d",
        description="Parsing format string for event time.",
    )
    knowledge_time: KnowledgeTimeConfig = Field(
        ...,
        description="Configuration for knowledge timestamp.",
    )
    timezone: str = Field(
        default="UTC",
        description="Source timezone identifier for naive timestamps.",
    )
    value_columns: list[str] = Field(
        ...,
        min_length=1,
        description="Columns carrying metric values.",
    )
    unit_column: str | None = Field(
        default=None,
        description="Optional column specifying measurement unit.",
    )
    field_columns: list[str] = Field(
        default_factory=list,
        description="Columns concatenated to form canonical field.",
    )
    expected_calendar: ExpectedCalendar = Field(
        default=ExpectedCalendar.NONE,
        description="Expected reporting period cadence.",
    )
    expected_universe: Path | None = Field(
        default=None,
        description="Optional path to universe entity list.",
    )
    rules: Path | None = Field(
        default=None,
        description="Relative path to validation rules YAML file.",
    )

    @field_validator("timezone")
    @classmethod
    def validate_timezone(cls, v: str) -> str:
        try:
            zoneinfo.ZoneInfo(v)
        except Exception as exc:
            raise ValueError(f"Invalid IANA timezone identifier: {v}") from exc
        return v

    @classmethod
    def from_yaml(cls, content: str) -> VendorManifest:
        data: Any = yaml.safe_load(content)
        if not isinstance(data, dict):
            raise ValueError("Manifest content must be a YAML mapping.")
        return cls.model_validate(data)

    @classmethod
    def from_file(cls, path: Path | str) -> VendorManifest:
        p = Path(path)
        if not p.exists():
            raise FileNotFoundError(f"Vendor manifest file not found: {p}")
        return cls.from_yaml(p.read_text(encoding="utf-8"))

    def to_yaml(self) -> str:
        dumped = self.model_dump(mode="json", exclude_none=False)
        return yaml.safe_dump(dumped, sort_keys=False)

    def to_file(self, path: Path | str) -> None:
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(self.to_yaml(), encoding="utf-8")
