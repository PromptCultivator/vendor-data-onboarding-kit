"""vendor-data-onboarding-kit: Institutional-grade research infrastructure and market data platform."""

from vendor_onboarding.api import (
    DiffError,
    IngestionError,
    SchemaValidationError,
    StorageError,
    SymbologyError,
    VendorKitError,
    diff_drops,
    infer_and_pin,
    query_as_of,
    run_onboarding_pipeline,
    validate_drop,
)
from vendor_onboarding.datasets import (
    AlfredVintageLoader,
    SECDeraLoader,
    SyntheticGenerator,
    as_of,
    generate_multi_drop_suite,
    generate_synthetic_drop,
    load_alfred_raw,
    load_alfred_series,
    load_sec_dera,
    load_sec_dera_facts,
)
from vendor_onboarding.diff import (
    DeltaStatistics,
    DiffEngine,
    DiffSummary,
    RevisionLedger,
)
from vendor_onboarding.flight import (
    BitemporalFlightClient,
    BitemporalFlightServer,
    create_flight_ticket,
    parse_flight_ticket,
)
from vendor_onboarding.ingestion import (
    DropManifest,
    create_manifest,
    infer_schema,
    read_file,
)
from vendor_onboarding.models import (
    CanonicalRecord,
    ColumnDefinition,
    DataType,
    DriftReport,
    SchemaDefinition,
)
from vendor_onboarding.storage import (
    BitemporalParquetStore,
    BitemporalQueryEngine,
    BitemporalStore,
    StorageFactorReport,
)
from vendor_onboarding.symbology import (
    QuarantineRecord,
    SymbologyMapping,
    SymbologyMaster,
)
from vendor_onboarding.validation import (
    ValidationEngine,
    ValidationResult,
    VectorizedValidationEngine,
)

__version__ = "0.1.0"

__all__ = [
    "AlfredVintageLoader",
    "BitemporalFlightClient",
    "BitemporalFlightServer",
    "BitemporalParquetStore",
    "BitemporalQueryEngine",
    "BitemporalStore",
    "CanonicalRecord",
    "ColumnDefinition",
    "DataType",
    "DeltaStatistics",
    "DiffEngine",
    "DiffError",
    "DiffSummary",
    "DriftReport",
    "DropManifest",
    "IngestionError",
    "QuarantineRecord",
    "RevisionLedger",
    "SECDeraLoader",
    "SchemaDefinition",
    "SchemaValidationError",
    "StorageError",
    "StorageFactorReport",
    "SymbologyError",
    "SymbologyMapping",
    "SymbologyMaster",
    "SyntheticGenerator",
    "ValidationEngine",
    "ValidationResult",
    "VectorizedValidationEngine",
    "VendorKitError",
    "__version__",
    "as_of",
    "create_flight_ticket",
    "create_manifest",
    "diff_drops",
    "generate_multi_drop_suite",
    "generate_synthetic_drop",
    "infer_and_pin",
    "infer_schema",
    "load_alfred_raw",
    "load_alfred_series",
    "load_sec_dera",
    "load_sec_dera_facts",
    "parse_flight_ticket",
    "query_as_of",
    "read_file",
    "run_onboarding_pipeline",
    "validate_drop",
]
