"""Unit tests for vendor manifest model, validation, and serialization."""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from vdok.manifest import (
    EntityIdType,
    ExpectedCalendar,
    KnowledgeTimeConfig,
    KnowledgeTimeSource,
    VendorManifest,
)

SAMPLE_VALID_MANIFEST_YAML = """
vendor: sec_fsds
entity_columns: [cik]
entity_id_type: cik
event_time_column: ddate
event_time_format: "%Y%m%d"
knowledge_time:
  source: column
  column: filed
  format: "%Y%m%d"
timezone: America/New_York
value_columns: [value]
unit_column: uom
field_columns: [tag]
expected_calendar: quarterly
rules: rules/sec_fsds.yaml
"""


def test_manifest_from_yaml_valid() -> None:
    manifest = VendorManifest.from_yaml(SAMPLE_VALID_MANIFEST_YAML)
    assert manifest.vendor == "sec_fsds"
    assert manifest.entity_columns == ["cik"]
    assert manifest.entity_id_type == EntityIdType.CIK
    assert manifest.event_time_column == "ddate"
    assert manifest.event_time_format == "%Y%m%d"
    assert manifest.knowledge_time.source == KnowledgeTimeSource.COLUMN
    assert manifest.knowledge_time.column == "filed"
    assert manifest.knowledge_time.format == "%Y%m%d"
    assert manifest.timezone == "America/New_York"
    assert manifest.value_columns == ["value"]
    assert manifest.unit_column == "uom"
    assert manifest.field_columns == ["tag"]
    assert manifest.expected_calendar == ExpectedCalendar.QUARTERLY
    assert manifest.rules == Path("rules/sec_fsds.yaml")


def test_manifest_roundtrip_file(tmp_path: Path) -> None:
    manifest = VendorManifest.from_yaml(SAMPLE_VALID_MANIFEST_YAML)
    target_file = tmp_path / "manifests" / "sec_fsds.yaml"
    manifest.to_file(target_file)
    assert target_file.exists()

    loaded = VendorManifest.from_file(target_file)
    assert loaded.vendor == manifest.vendor
    assert loaded.entity_id_type == manifest.entity_id_type
    assert loaded.timezone == manifest.timezone
    assert loaded.value_columns == manifest.value_columns


def test_manifest_from_file_missing_raises() -> None:
    with pytest.raises(FileNotFoundError):
        VendorManifest.from_file("/path/that/does/not/exist/manifest.yaml")


def test_manifest_from_yaml_non_dict_raises() -> None:
    with pytest.raises(ValueError, match="YAML mapping"):
        VendorManifest.from_yaml("['list', 'is', 'not', 'mapping']")


def test_timezone_validation_success() -> None:
    for tz_name in ("UTC", "America/New_York", "Europe/London", "Asia/Tokyo"):
        m = VendorManifest.model_validate(
            {
                "vendor": "test_vendor",
                "entity_columns": ["id"],
                "entity_id_type": "ticker",
                "event_time_column": "time",
                "knowledge_time": {"source": "drop_time"},
                "timezone": tz_name,
                "value_columns": ["val"],
            }
        )
        assert m.timezone == tz_name


def test_timezone_validation_invalid_raises() -> None:
    with pytest.raises(ValidationError, match="Invalid IANA timezone"):
        VendorManifest.model_validate(
            {
                "vendor": "test_vendor",
                "entity_columns": ["id"],
                "entity_id_type": "ticker",
                "event_time_column": "time",
                "knowledge_time": {"source": "drop_time"},
                "timezone": "Invalid/Unknown_Zone_123",
                "value_columns": ["val"],
            }
        )


def test_knowledge_time_column_requirement() -> None:
    # source: column without column specified must raise
    with pytest.raises(ValidationError, match="requires 'column' name"):
        KnowledgeTimeConfig.model_validate({"source": "column"})

    # source: column with column specified succeeds
    kt = KnowledgeTimeConfig.model_validate({"source": "column", "column": "as_of"})
    assert kt.column == "as_of"

    # source: drop_time allows column to be omitted
    kt_drop = KnowledgeTimeConfig.model_validate({"source": "drop_time"})
    assert kt_drop.source == KnowledgeTimeSource.DROP_TIME
    assert kt_drop.column is None


def test_vendor_name_pattern_validation() -> None:
    valid_vendors = ["sec_fsds", "vendor-1", "vendor_2024", "simple123"]
    for v in valid_vendors:
        m = VendorManifest.model_validate(
            {
                "vendor": v,
                "entity_columns": ["id"],
                "entity_id_type": "vendor_id",
                "event_time_column": "time",
                "knowledge_time": {"source": "drop_time"},
                "value_columns": ["val"],
            }
        )
        assert m.vendor == v

    invalid_vendors = ["SEC_FSDS", "vendor with spaces", "vendor#bad!"]
    for v in invalid_vendors:
        with pytest.raises(ValidationError):
            VendorManifest.model_validate(
                {
                    "vendor": v,
                    "entity_columns": ["id"],
                    "entity_id_type": "vendor_id",
                    "event_time_column": "time",
                    "knowledge_time": {"source": "drop_time"},
                    "value_columns": ["val"],
                }
            )


def test_empty_column_lists_raise() -> None:
    with pytest.raises(ValidationError):
        VendorManifest.model_validate(
            {
                "vendor": "test_vendor",
                "entity_columns": [],
                "entity_id_type": "ticker",
                "event_time_column": "time",
                "knowledge_time": {"source": "drop_time"},
                "value_columns": ["val"],
            }
        )

    with pytest.raises(ValidationError):
        VendorManifest.model_validate(
            {
                "vendor": "test_vendor",
                "entity_columns": ["id"],
                "entity_id_type": "ticker",
                "event_time_column": "time",
                "knowledge_time": {"source": "drop_time"},
                "value_columns": [],
            }
        )


def test_entity_id_type_enum_values() -> None:
    for expected in ("cik", "ticker", "isin", "cusip", "vendor_id"):
        assert EntityIdType(expected) is not None

    with pytest.raises(ValueError):
        EntityIdType("unsupported_type")


def test_expected_calendar_enum_values() -> None:
    for expected in ("daily", "weekly", "monthly", "quarterly", "none"):
        assert ExpectedCalendar(expected) is not None

    with pytest.raises(ValueError):
        ExpectedCalendar("hourly")
