"""Deterministic generator for SEC FSDS test fixtures.

Generates two quarterly drops: 2023q3 and 2023q4, with real CIKs from
company_tickers.json, authentic filing tags (Revenues, NetIncomeLoss, Assets),
valid financial dates, and 25 planted restatement pairs across quarters.
"""

from __future__ import annotations

import json
from pathlib import Path

FIXTURES_DIR = Path(__file__).parent
SYMBOLS_FILE = FIXTURES_DIR / "symbols" / "company_tickers.json"
SEC_FIXTURES_DIR = FIXTURES_DIR / "sec_fsds"

SUB_HEADER = [
    "adsh",
    "cik",
    "name",
    "sic",
    "countryba",
    "stprba",
    "cityba",
    "zipba",
    "bas1",
    "bas2",
    "baph",
    "countryma",
    "stprma",
    "cityma",
    "zipma",
    "mas1",
    "mas2",
    "countryinc",
    "stprinc",
    "ein",
    "former",
    "changed",
    "afs",
    "wksi",
    "fye",
    "form",
    "period",
    "fy",
    "fp",
    "filed",
    "accepted",
    "prevrpt",
    "detail",
    "instance",
    "nciks",
    "aciks",
]

NUM_HEADER = ["adsh", "tag", "version", "coreg", "ddate", "qtrs", "uom", "value", "footnote"]

TAGS = ["Revenues", "NetIncomeLoss", "Assets", "Liabilities", "OperatingIncomeLoss"]


def generate_sec_fixtures() -> None:
    with open(SYMBOLS_FILE, "r", encoding="utf-8") as f:
        symbols_data = json.load(f)

    # Pick top 50 unique CIKs
    ciks: list[tuple[int, str]] = []
    seen = set()
    for entry in symbols_data.values():
        cik = int(entry["cik_str"])
        title = entry["title"]
        if cik not in seen and len(ciks) < 50:
            seen.add(cik)
            ciks.append((cik, title))

    q3_dir = SEC_FIXTURES_DIR / "2023q3"
    q4_dir = SEC_FIXTURES_DIR / "2023q4"
    q3_dir.mkdir(parents=True, exist_ok=True)
    q4_dir.mkdir(parents=True, exist_ok=True)

    # Drop 1: 2023q3 (Filed 2023-08-15, ddate 2023-06-30)
    q3_subs = []
    q3_nums = []

    # Drop 2: 2023q4 (Filed 2023-11-15, ddate 2023-09-30 and restatements for 2023-06-30)
    q4_subs = []
    q4_nums = []

    for idx, (cik, title) in enumerate(ciks, start=1):
        adsh_q3 = f"{cik:010d}-23-{idx:06d}"
        adsh_q4 = f"{cik:010d}-23-{idx + 500:06d}"

        # sub.txt row
        sub_row_q3 = [
            adsh_q3,
            str(cik),
            title,
            "7372",
            "US",
            "CA",
            "San Jose",
            "95134",
            "Market St",
            "",
            "555-0100",
            "US",
            "CA",
            "San Jose",
            "95134",
            "",
            "",
            "US",
            "DE",
            "123456789",
            "",
            "",
            "1-LAF",
            "1",
            "1231",
            "10-Q",
            "20230630",
            "2023",
            "Q2",
            "20230815",
            "2023-08-15 16:30:00",
            "0",
            "1",
            f"filing_{idx}.htm",
            "1",
            "",
        ]
        q3_subs.append("\t".join(sub_row_q3))

        sub_row_q4 = [
            adsh_q4,
            str(cik),
            title,
            "7372",
            "US",
            "CA",
            "San Jose",
            "95134",
            "Market St",
            "",
            "555-0100",
            "US",
            "CA",
            "San Jose",
            "95134",
            "",
            "",
            "US",
            "DE",
            "123456789",
            "",
            "",
            "1-LAF",
            "1",
            "1231",
            "10-Q",
            "20230930",
            "2023",
            "Q3",
            "20231115",
            "2023-11-15 16:30:00",
            "0",
            "1",
            f"filing_{idx}_q3.htm",
            "1",
            "",
        ]
        q4_subs.append("\t".join(sub_row_q4))

        # num.txt rows for Q3
        for tag_idx, tag in enumerate(TAGS):
            base_val = (cik % 1000 + idx * 100 + tag_idx * 50) * 1000000.00
            num_row_q3 = [
                adsh_q3,
                tag,
                "us-gaap/2023",
                "",
                "20230630",
                "2",
                "USD",
                f"{base_val:.4f}",
                "",
            ]
            q3_nums.append("\t".join(num_row_q3))

            # In Q4:
            # 1. New facts for 20230930
            new_val = base_val * 1.05
            num_row_q4_new = [
                adsh_q4,
                tag,
                "us-gaap/2023",
                "",
                "20230930",
                "3",
                "USD",
                f"{new_val:.4f}",
                "",
            ]
            q4_nums.append("\t".join(num_row_q4_new))

            # 2. Plant restatements: for the first 25 CIKs on tag "Revenues", restate 20230630!
            if idx <= 25 and tag == "Revenues":
                restated_val = base_val + 5000000.00
                num_row_q4_restate = [
                    adsh_q4,
                    tag,
                    "us-gaap/2023",
                    "",
                    "20230630",
                    "2",
                    "USD",
                    f"{restated_val:.4f}",
                    "",
                ]
                q4_nums.append("\t".join(num_row_q4_restate))

    # Write files
    (q3_dir / "sub.txt").write_text(
        "\t".join(SUB_HEADER) + "\n" + "\n".join(q3_subs) + "\n", encoding="utf-8"
    )
    (q3_dir / "num.txt").write_text(
        "\t".join(NUM_HEADER) + "\n" + "\n".join(q3_nums) + "\n", encoding="utf-8"
    )

    (q4_dir / "sub.txt").write_text(
        "\t".join(SUB_HEADER) + "\n" + "\n".join(q4_subs) + "\n", encoding="utf-8"
    )
    (q4_dir / "num.txt").write_text(
        "\t".join(NUM_HEADER) + "\n" + "\n".join(q4_nums) + "\n", encoding="utf-8"
    )

    print(f"Generated SEC fixtures: {len(q3_nums)} Q3 rows, {len(q4_nums)} Q4 rows across 50 CIKs.")


if __name__ == "__main__":
    generate_sec_fixtures()
