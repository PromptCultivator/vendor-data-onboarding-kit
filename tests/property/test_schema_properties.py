"""Property-based tests using Hypothesis for schema engine and discovery guarantees."""

from __future__ import annotations

import random
import zoneinfo
from decimal import Decimal

from hypothesis import given
from hypothesis import strategies as st

from vdok.discover import compute_drop_id
from vdok.manifest import VendorManifest
from vdok.schema import (
    ColumnSchema,
    DriftCategory,
    DriftSeverity,
    InferredType,
    VendorSchema,
    _decimal_prec_scale,
    compare_schemas,
)


@given(
    st.lists(
        st.text(alphabet="0123456789abcdef", min_size=64, max_size=64),
        min_size=1,
        max_size=20,
    )
)
def test_drop_id_permutation_invariance(hashes: list[str]) -> None:
    """Drop ID must be invariant to the order in which file hashes are presented."""
    shuffled = list(hashes)
    random.shuffle(shuffled)
    assert compute_drop_id(hashes) == compute_drop_id(shuffled)


@given(
    st.decimals(
        min_value=Decimal("-1000000000"),
        max_value=Decimal("1000000000"),
        allow_nan=False,
        allow_infinity=False,
        places=4,
    )
)
def test_decimal_scale_exactness(d: Decimal) -> None:
    """Decimal scale extraction preserves fixed decimal digits without floating point coercion."""
    prec, scale = _decimal_prec_scale(d)
    assert scale <= 4
    assert prec >= 1


@given(st.sampled_from(sorted(zoneinfo.available_timezones())))
def test_manifest_iana_timezone_property(tz_name: str) -> None:
    """Any recognized IANA timezone passes manifest validation."""
    m = VendorManifest.model_validate(
        {
            "vendor": "test_vendor",
            "entity_columns": ["entity_id"],
            "entity_id_type": "ticker",
            "event_time_column": "event_time",
            "knowledge_time": {"source": "drop_time"},
            "timezone": tz_name,
            "value_columns": ["val"],
        }
    )
    assert m.timezone == tz_name


@given(
    st.lists(
        st.text(alphabet="abcdefghijklmnopqrstuvwxyz_", min_size=1, max_size=15),
        min_size=1,
        max_size=10,
        unique=True,
    )
)
def test_schema_diff_reflexive_identity(col_names: list[str]) -> None:
    """Comparing any schema against itself produces 0 differences and has_hold=False."""
    columns = {col: ColumnSchema(type=InferredType.STRING, nullable=True) for col in col_names}
    schema = VendorSchema(
        vendor="vendor_idempotent",
        version=1,
        pinned_at="2024-01-01T00:00:00Z",
        columns=columns,
    )
    diff = compare_schemas(schema, schema)
    assert len(diff.differences) == 0
    assert diff.has_hold is False


@given(
    st.text(alphabet="abcdefghijklmnopqrstuvwxyz_", min_size=1, max_size=15),
    st.text(alphabet="abcdefghijklmnopqrstuvwxyz_", min_size=1, max_size=15),
)
def test_schema_drift_added_removed_invariants(col_a: str, col_b: str) -> None:
    """Adding a column is always WARN, removing a column is always HOLD."""
    if col_a == col_b:
        return

    base_cols = {col_a: ColumnSchema(type=InferredType.INTEGER, nullable=False)}
    expanded_cols = {
        col_a: ColumnSchema(type=InferredType.INTEGER, nullable=False),
        col_b: ColumnSchema(type=InferredType.STRING, nullable=True),
    }

    base_schema = VendorSchema(
        vendor="v", version=1, pinned_at="2024-01-01T00:00:00Z", columns=base_cols
    )
    expanded_schema = VendorSchema(
        vendor="v", version=1, pinned_at="2024-01-01T00:00:00Z", columns=expanded_cols
    )

    # Added column: non-breaking warn
    diff_added = compare_schemas(base_schema, expanded_schema)
    assert diff_added.has_hold is False
    assert any(
        d.category == DriftCategory.ADDED_COLUMN and d.severity == DriftSeverity.WARN
        for d in diff_added.differences
    )

    # Removed column: breaking hold
    diff_removed = compare_schemas(expanded_schema, base_schema)
    assert diff_removed.has_hold is True
    assert any(
        d.category == DriftCategory.REMOVED_COLUMN and d.severity == DriftSeverity.HOLD
        for d in diff_removed.differences
    )
