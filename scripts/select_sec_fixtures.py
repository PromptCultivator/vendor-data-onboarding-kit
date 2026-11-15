#!/usr/bin/env python3
"""Deterministic fixture selection script for SEC Financial Statement Data Sets.

Samples authentic SEC quarterly releases to construct test fixtures covering
at least 50 CIKs and at least 20 revision pairs across consecutive quarters.
Documented in tests/fixtures/README.md.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from vdok.adapters.sec_fsds import select_sec_fixtures


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Select deterministic SEC FSDS fixtures for continuous integration."
    )
    parser.add_argument(
        "--q3",
        type=str,
        default="data/raw/sec_fsds/2023q3",
        help="Path to 2023q3 drop directory or archive",
    )
    parser.add_argument(
        "--q4",
        type=str,
        default="data/raw/sec_fsds/2023q4",
        help="Path to 2023q4 drop directory or archive",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="tests/fixtures/sec_fsds",
        help="Target fixtures output directory",
    )
    parser.add_argument(
        "--target-ciks",
        type=int,
        default=50,
        help="Target count of distinct corporate entities (CIKs)",
    )
    parser.add_argument(
        "--target-num-rows",
        type=int,
        default=20000,
        help="Target count of numerical disclosure records",
    )
    parser.add_argument(
        "--min-revisions",
        type=int,
        default=20,
        help="Minimum restatement pairs required across quarters",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_path = Path(args.output_dir)
    print(f"Selecting SEC FSDS fixtures into {output_path}...")
    stats = select_sec_fixtures(
        q3_path=args.q3,
        q4_path=args.q4,
        output_dir=output_path,
        target_ciks=args.target_ciks,
        target_num_rows=args.target_num_rows,
        min_revision_pairs=args.min_revisions,
    )
    print("Fixture selection summary:")
    for k, v in stats.items():
        print(f"  {k}: {v}")


if __name__ == "__main__":
    main()
