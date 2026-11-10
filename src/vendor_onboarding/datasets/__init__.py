"""Institutional market dataset loaders and synthetic generator suite."""

from vendor_onboarding.datasets.fred import (
    AlfredVintageLoader,
    as_of,
    load_alfred_raw,
    load_alfred_series,
)
from vendor_onboarding.datasets.sec import (
    SECDeraLoader,
    load_sec_dera,
    load_sec_dera_facts,
)
from vendor_onboarding.datasets.synthetic import (
    SyntheticGenerator,
    generate_multi_drop_suite,
    generate_synthetic_drop,
)

__all__ = [
    "AlfredVintageLoader",
    "SECDeraLoader",
    "SyntheticGenerator",
    "as_of",
    "generate_multi_drop_suite",
    "generate_synthetic_drop",
    "load_alfred_raw",
    "load_alfred_series",
    "load_sec_dera",
    "load_sec_dera_facts",
]
