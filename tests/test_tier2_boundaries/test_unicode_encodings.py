"""
Tier 2 Boundary Tests: Unicode Encodings, BOM Marks, CJK Multi-Byte, Emojis, and Line Endings.

Per TEST_INFRA.md and PROJECT.md §8.1, §8.2 / F-ING-01, F-MOD-01, F-VAL-02:
Tests international character set and text encoding boundary conditions:
- UTF-8 Byte Order Mark (BOM: \xef\xbb\xbf) stripping without header corruption
- Latin-1 / ISO-8859-1 accented characters (é, ü, ñ, ç, ø, ä)
- Windows-1252 high-order code points (smart quotes \x93/\x94, em-dash \x97, euro \x80)
- Multi-byte CJK (Chinese, Japanese, Korean) company names and symbols
- 4-byte UTF-8 emojis in unstructured text and financial sentiment feeds
- Embedded NUL bytes (\x00) and truncation hazards
- Mixed line endings (CRLF, LF, CR) and multiline quoted CSV fields
- Unicode canonical normalization (NFC vs NFD equivalence)
- Zero-width spaces and control character sanitization
- Right-to-Left (RTL) Arabic and Hebrew company entity resolution
"""

from __future__ import annotations

import csv
import hashlib
import json
import unicodedata
from pathlib import Path
from typing import Any

import pytest

try:
    import duckdb
except ImportError:
    duckdb = None  # type: ignore

try:
    import pyarrow as pa
    import pyarrow.csv as pcsv
    import pyarrow.parquet as pq
except ImportError:
    pa = None  # type: ignore
    pq = None  # type: ignore


pytestmark = [pytest.mark.tier2]


# ============================================================================
# 1. UTF-8 Byte Order Mark (BOM)
# ============================================================================

def test_utf8_bom_csv_header_not_corrupted(tmp_path: Path):
    """CSV with UTF-8 BOM (\xef\xbb\xbf) must not leave \ufeff in the first column name."""
    bom_csv = tmp_path / "with_bom.csv"
    # Write BOM followed by standard header and row
    bom_bytes = b"\xef\xbb\xbfentity_id,event_time,value\nAAPL,2023-10-01,150.0\n"
    bom_csv.write_bytes(bom_bytes)

    # In Python with utf-8-sig
    with open(bom_csv, "r", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        header = next(reader)
        assert header[0] == "entity_id", f"BOM corrupted column: {header[0]}"

    # In DuckDB
    if duckdb:
        conn = duckdb.connect(":memory:")
        res = conn.sql(f"SELECT * FROM read_csv('{bom_csv}', header=true)").to_arrow_table()
        first_col = res.schema.names[0]
        assert first_col == "entity_id", f"DuckDB kept BOM: {repr(first_col)}"
        conn.close()


def test_utf8_bom_json_parsing(tmp_path: Path):
    """JSON file with UTF-8 BOM must decode cleanly when using utf-8-sig."""
    bom_json = tmp_path / "with_bom.json"
    content = json.dumps({"vendor_id": "TEST_V", "drop_id": "DROP_BOM"}).encode("utf-8")
    bom_json.write_bytes(b"\xef\xbb\xbf" + content)

    # Reading with utf-8-sig strips BOM
    parsed = json.loads(bom_json.read_bytes().decode("utf-8-sig"))
    assert parsed["vendor_id"] == "TEST_V"
    assert parsed["drop_id"] == "DROP_BOM"


def test_utf8_bom_duckdb_read_csv(tmp_path: Path):
    """DuckDB natively ignores UTF-8 BOM in read_csv."""
    bom_csv = tmp_path / "bom_duck.csv"
    bom_csv.write_bytes(b"\xef\xbb\xbfid,val\n1,100\n2,200\n")

    if duckdb:
        conn = duckdb.connect(":memory:")
        res = conn.execute(f"SELECT id, val FROM read_csv('{bom_csv}', header=true)").fetchall()
        assert res == [(1, 100), (2, 200)]
        conn.close()


# ============================================================================
# 2. Latin-1 / ISO-8859-1 & Windows-1252 Encodings
# ============================================================================

def test_latin1_encoded_csv_file_reading(tmp_path: Path):
    """CSV containing European accented characters in ISO-8859-1."""
    latin1_csv = tmp_path / "latin1.csv"
    # "Crédit Agricole", "Société Générale", "Nestlé"
    text = "entity_id,company_name,country\nACA,Crédit Agricole,FR\nGLE,Société Générale,FR\nNESN,Nestlé,CH\n"
    latin1_csv.write_bytes(text.encode("iso-8859-1"))

    # Decoding with iso-8859-1 reproduces original accents
    decoded = latin1_csv.read_bytes().decode("iso-8859-1")
    assert "Crédit" in decoded
    assert "Société" in decoded
    assert "Nestlé" in decoded


def test_latin1_accented_characters_preservation(tmp_path: Path):
    """Accented characters é, ü, ñ, ç, ø, ä roundtrip cleanly through Parquet."""
    names = ["Münchener Rück", "Banco Santander España", "Novo Nordisk København", "Curaçao Holdings"]
    table = pa.table({"company": names})

    pq_path = tmp_path / "accented.parquet"
    pq.write_table(table, pq_path)

    read_table = pq.read_table(pq_path)
    assert read_table["company"].to_pylist() == names


def test_windows_1252_smart_quotes_and_symbols(tmp_path: Path):
    """Windows-1252 high-order characters (e.g. smart double quotes \x93, \x94, em-dash \x97)."""
    win1252_csv = tmp_path / "win1252.csv"
    # \x93 = left double quote, \x94 = right double quote, \x97 = em-dash
    raw_bytes = b"id,text\n1,\x93Special Corp\x94 \x97 Restated\n"
    win1252_csv.write_bytes(raw_bytes)

    # Decode with cp1252
    decoded = raw_bytes.decode("cp1252")
    assert "“Special Corp” — Restated" == decoded.split("\n")[1].split(",")[1]


def test_windows_1252_euro_symbol_byte_80():
    """Byte 0x80 in Windows-1252 is the Euro symbol (€). In ISO-8859-1 it is an unassigned control char."""
    euro_byte = b"\x80"
    euro_char = euro_byte.decode("cp1252")
    assert euro_char == "€"
    assert ord(euro_char) == 8364


# ============================================================================
# 3. Multi-Byte CJK Company Names and Symbols
# ============================================================================

def test_cjk_chinese_company_names_in_canonical_records(synthetic_record_factory):
    """CanonicalRecord supports Chinese Simplified and Traditional company names."""
    cjk_names = [
        ("ENT_700", "腾讯控股有限公司", "0700.HK"),
        ("ENT_9988", "阿里巴巴集团控股有限公司", "9988.HK"),
        ("ENT_939", "中国建设银行", "0939.HK"),
    ]

    for ent_id, name, symbol in cjk_names:
        rec = synthetic_record_factory(
            entity_id=ent_id,
            source_symbol=symbol,
            unit=name,  # test unicode in field
        )
        assert rec.unit == name
        assert len(name) > 0


def test_cjk_japanese_katakana_kanji_symbols():
    """Japanese corporate names in Kanji and Katakana."""
    jp_companies = [
        "トヨタ自動車株式会社",  # Toyota
        "ソニーグループ株式会社",  # Sony
        "任天堂株式会社",  # Nintendo
        "ソフトバンクグループ",  # SoftBank
    ]

    arr = pa.array(jp_companies, type=pa.string())
    assert arr.to_pylist() == jp_companies
    assert len(arr) == 4


def test_cjk_korean_hangul_descriptions():
    """Korean Hangul characters in descriptions and security titles."""
    kr_companies = ["삼성전자", "현대자동차", "카카오"]
    for name in kr_companies:
        utf8_encoded = name.encode("utf-8")
        # In UTF-8 each Korean Hangul character is 3 bytes
        assert len(utf8_encoded) == len(name) * 3

    # Mixed ASCII and Hangul
    mixed = "SK하이닉스"
    assert len(mixed.encode("utf-8")) == 2 + (4 * 3)


def test_cjk_multibyte_character_count_vs_byte_length():
    """Character count vs byte length distinction in multi-byte encodings."""
    chinese_symbol = "百度"  # Baidu: 2 characters
    assert len(chinese_symbol) == 2
    assert len(chinese_symbol.encode("utf-8")) == 6  # 3 bytes per char in UTF-8


def test_cjk_string_slicing_by_characters_not_bytes():
    """String truncation rules must slice by unicode code points, never splitting multi-byte sequence."""
    long_desc = "中国平安保险（集团）股份有限公司是一家多元化金融服务集团"
    # Character slice of first 10 code points
    truncated = long_desc[:10]
    assert truncated == "中国平安保险（集团）"
    # Slicing should not raise UnicodeDecodeError when encoded back
    assert truncated.encode("utf-8").decode("utf-8") == truncated


# ============================================================================
# 4. Emojis and 4-Byte UTF-8 Sequences
# ============================================================================

def test_emoji_4byte_utf8_financial_sentiment_text():
    """4-byte UTF-8 emojis commonly found in social sentiment or alternative data feeds."""
    sentiment_texts = [
        "Strong earnings beat! 🚀📈💰",
        "Dow plunged 500 points 📉⚠️",
        "Corporate headquarters tour 🏢🤖",
    ]

    for text in sentiment_texts:
        encoded = text.encode("utf-8")
        decoded = encoded.decode("utf-8")
        assert text == decoded
        # Verify 4-byte sequence presence
        assert any(len(ch.encode("utf-8")) == 4 for ch in text)


def test_emoji_preservation_in_parquet_roundtrip(tmp_path: Path):
    """Parquet columns preserve emojis and 4-byte UTF-8 without stripping or truncation."""
    fpath = tmp_path / "emojis.parquet"
    data = ["Bull market 🚀", "Bear market 🐻", "Cash flow 💸"]
    table = pa.table({"sentiment": data})

    pq.write_table(table, fpath)
    res = pq.read_table(fpath)
    assert res["sentiment"].to_pylist() == data


def test_emoji_in_duckdb_sql_queries(temp_duckdb_conn):
    """DuckDB SQL executing filters and string functions over 4-byte emojis."""
    temp_duckdb_conn.execute("""
        CREATE TABLE sentiments (id INT, tweet VARCHAR);
        INSERT INTO sentiments VALUES 
            (1, 'Long $AAPL 🚀🚀'),
            (2, 'Short $SPY 📉'),
            (3, 'Neutral hold ⚖️');
    """)

    res = temp_duckdb_conn.execute("SELECT tweet FROM sentiments WHERE tweet LIKE '%🚀%'").fetchall()
    assert len(res) == 1
    assert "🚀🚀" in res[0][0]


# ============================================================================
# 5. Embedded NUL Bytes (\x00) & Sanitization
# ============================================================================

def test_embedded_null_byte_in_csv_field_detection():
    """Detecting embedded NUL bytes inside text fields."""
    raw_str = "AAPL\x00INJECTED_SUFFIX"
    assert "\x00" in raw_str

    # Sanitization: strip NUL or reject
    sanitized = raw_str.replace("\x00", "")
    assert sanitized == "AAPLINJECTED_SUFFIX"


def test_embedded_null_byte_in_json_field():
    """JSON with embedded \u0000 string code point."""
    json_str = '{"entity_id": "MSFT\\u0000_CORP"}'
    parsed = json.loads(json_str)
    assert "\x00" in parsed["entity_id"]
    assert len(parsed["entity_id"]) == 10


def test_embedded_null_byte_in_parquet_string_roundtrip(tmp_path: Path):
    """Parquet preserves strings containing embedded null bytes."""
    fpath = tmp_path / "null_bytes.parquet"
    val = "VAL\x00MID"
    table = pa.table({"code": [val]})

    pq.write_table(table, fpath)
    res = pq.read_table(fpath)
    assert res["code"][0].as_py() == val


# ============================================================================
# 6. Mixed Line Endings & Quoted Multiline Text
# ============================================================================

def test_mixed_crlf_and_lf_line_endings_in_csv(tmp_path: Path):
    """CSV file containing mixed CRLF and LF lines parsed via universal newlines."""
    fpath = tmp_path / "mixed_endings.csv"
    # Row 1 CRLF, Row 2 LF, Row 3 CRLF
    content = b"id,val\r\n1,100\n2,200\r\n3,300\n"
    fpath.write_bytes(content)

    # Universal newline reading standardizes all line terminations
    with open(fpath, "r", newline=None, encoding="utf-8") as f:
        reader = list(csv.reader(f))
        assert len(reader) == 4
        assert reader[0] == ["id", "val"]
        assert reader[1] == ["1", "100"]
        assert reader[2] == ["2", "200"]
        assert reader[3] == ["3", "300"]

    # Pre-normalizing line endings allows strict columnar parsers to ingest cleanly
    normalized_path = tmp_path / "normalized.csv"
    normalized_path.write_bytes(content.replace(b"\r\n", b"\n").replace(b"\r", b"\n"))
    if duckdb:
        conn = duckdb.connect(":memory:")
        res = conn.execute(f"SELECT * FROM read_csv('{normalized_path}', header=true)").fetchall()
        assert len(res) == 3
        conn.close()


def test_classic_mac_cr_only_line_endings(tmp_path: Path):
    """CSV using classic Mac OS line endings (\r only)."""
    fpath = tmp_path / "cr_only.csv"
    content = b"id,val\r1,10\r2,20\r"
    fpath.write_bytes(content)

    # In Python with universal newlines
    with open(fpath, "r", newline=None, encoding="utf-8") as f:
        reader = list(csv.reader(f))
        assert len(reader) == 3
        assert reader[0] == ["id", "val"]
        assert reader[1] == ["1", "10"]


def test_quoted_multiline_csv_with_crlf(tmp_path: Path):
    """CSV with embedded CRLF newlines inside double-quoted text fields."""
    fpath = tmp_path / "quoted_crlf.csv"
    content = (
        'id,description,val\r\n'
        '1,"Line 1\r\nLine 2\r\nLine 3",100.0\r\n'
        '2,"Single line",200.0\r\n'
    )
    fpath.write_bytes(content.encode("utf-8"))

    if duckdb:
        conn = duckdb.connect(":memory:")
        res = conn.execute(f"SELECT id, description, val FROM read_csv('{fpath}', header=true)").fetchall()
        assert len(res) == 2
        assert "Line 1\r\nLine 2\r\nLine 3" in res[0][1] or "Line 1\nLine 2\nLine 3" in res[0][1]
        assert res[1][2] == 200.0
        conn.close()


def test_quoted_multiline_csv_with_lf(tmp_path: Path):
    """CSV with embedded LF newlines inside double quotes."""
    fpath = tmp_path / "quoted_lf.csv"
    content = (
        'id,note\n'
        '1,"Item A\nItem B"\n'
        '2,"Item C"\n'
    )
    fpath.write_text(content, encoding="utf-8")

    import pyarrow.csv as pcsv
    tab = pcsv.read_csv(fpath)
    assert tab.num_rows == 2
    assert "Item A\nItem B" in tab["note"][0].as_py()


def test_quoted_multiline_jsonl(tmp_path: Path):
    """JSONL with escaped newlines inside strings parses as single lines."""
    fpath = tmp_path / "escaped_newlines.jsonl"
    lines = [
        json.dumps({"id": 1, "text": "Paragraph 1\n\nParagraph 2"}),
        json.dumps({"id": 2, "text": "Single paragraph"}),
    ]
    fpath.write_text("\n".join(lines), encoding="utf-8")

    records = [json.loads(line) for line in fpath.read_text(encoding="utf-8").splitlines()]
    assert len(records) == 2
    assert "\n\n" in records[0]["text"]


# ============================================================================
# 7. Unicode Normalization: NFC vs NFD
# ============================================================================

def test_unicode_nfc_vs_nfd_normalization_in_entity_lookup():
    """Verify that canonical composed (NFC) and decomposed (NFD) represent same entity."""
    # "Société" has two 'é' characters:
    # NFC represents each as single code point \u00e9 (len = 7)
    # NFD represents each as 'e' + \u0301 (len = 7 + 2 = 9)
    s_nfc = unicodedata.normalize("NFC", "Société")
    s_nfd = unicodedata.normalize("NFD", "Société")

    assert s_nfc != s_nfd  # Different byte/code sequences
    assert len(s_nfc) == 7
    assert len(s_nfd) == 9

    # Normalization to NFC resolves equivalence
    assert unicodedata.normalize("NFC", s_nfd) == s_nfc


def test_zero_width_space_in_symbol_sanitization():
    """Zero-width space (\\u200b) inadvertently pasted into ticker symbol must be detected."""
    dirty_symbol = "AAPL\u200b"
    assert len(dirty_symbol) == 5
    assert dirty_symbol != "AAPL"

    # Sanitizer removes zero-width spaces
    clean_symbol = dirty_symbol.replace("\u200b", "").strip()
    assert clean_symbol == "AAPL"


def test_non_breaking_space_in_numeric_string_parsing():
    """Non-breaking space (\\u00a0) used as thousand separator in French data (e.g. '1\\u00a0000,50')."""
    french_num = "1\u00a0000,50"
    # Normalize by replacing non-breaking space with empty string and comma with dot
    normalized = french_num.replace("\u00a0", "").replace(" ", "").replace(",", ".")
    val = float(normalized)
    assert val == 1000.50


# ============================================================================
# 8. Right-to-Left (RTL) Arabic & Hebrew Entities
# ============================================================================

def test_right_to_left_arabic_company_names():
    """Arabic company names (Saudi Aramco: أرامكو السعودية) roundtrip correctly."""
    aramco = "أرامكو السعودية"
    arr = pa.array([aramco], type=pa.string())
    assert arr[0].as_py() == aramco
    assert len(aramco) == 15


def test_right_to_left_hebrew_symbols():
    """Hebrew company names (Elbit Systems: אלביט מערכות)."""
    elbit = "אלביט מערכות"
    arr = pa.array([elbit], type=pa.string())
    assert arr[0].as_py() == elbit


def test_invalid_utf8_byte_sequence_handling():
    """Handling invalid / malformed UTF-8 byte sequences (e.g. bare continuation byte 0x80 or 0xFF)."""
    bad_bytes = b"entity_id,name\nAAPL,Apple\xff\xfeInc\n"
    with pytest.raises(UnicodeDecodeError):
        bad_bytes.decode("utf-8")

    # Decoding with errors='replace' inserts replacement character \ufffd
    safe_decoded = bad_bytes.decode("utf-8", errors="replace")
    assert "\ufffd" in safe_decoded


def test_sec_deras_special_characters_in_company_names(fixture_paths):
    """SEC DERA sub.txt filers containing &, commas, and corporate punctuation."""
    assert fixture_paths.sec_2023q3_sub.is_file()
    if duckdb:
        conn = duckdb.connect(":memory:")
        res = conn.execute(f"SELECT name FROM read_csv('{fixture_paths.sec_2023q3_sub}', delim='\t', header=true)").fetchall()
        assert len(res) == 3
        # E.g. 'APPLE INC', 'MICROSOFT CORP'
        assert any("INC" in r[0] for r in res)
        conn.close()


def test_alfred_notes_with_extended_unicode(fixture_paths):
    """ALFRED macroeconomic series files have clean headers."""
    assert fixture_paths.alfred_cpiaucsl.is_file()
    if duckdb:
        conn = duckdb.connect(":memory:")
        res = conn.execute(f"SELECT count(*) FROM read_csv('{fixture_paths.alfred_cpiaucsl}', header=true)").fetchall()[0][0]
        assert res == 24
        conn.close()


def test_sha256_manifest_exactness_across_line_ending_changes(tmp_path: Path):
    """Line ending conversion CRLF -> LF strictly alters file SHA-256 hash."""
    f_crlf = tmp_path / "crlf.csv"
    f_lf = tmp_path / "lf.csv"

    content = "entity_id,value\nAAPL,100\n"
    f_lf.write_bytes(content.encode("utf-8"))
    f_crlf.write_bytes(content.replace("\n", "\r\n").encode("utf-8"))

    h_lf = hashlib.sha256(f_lf.read_bytes()).hexdigest()
    h_crlf = hashlib.sha256(f_crlf.read_bytes()).hexdigest()

    assert h_lf != h_crlf, "Line ending change must modify SHA-256 hash"


def test_tab_delimited_with_embedded_spaces_and_unicode(tmp_path: Path):
    """Tab-delimited files (TSV) with embedded spaces and unicode strings."""
    tsv_file = tmp_path / "unicode.tsv"
    tsv_file.write_text("symbol\tname\tcountry\n700\t腾讯 控股\t中国\n", encoding="utf-8")

    if duckdb:
        conn = duckdb.connect(":memory:")
        res = conn.execute(f"SELECT * FROM read_csv('{tsv_file}', delim='\t', header=true)").fetchall()[0]
        assert res[0] == 700
        assert res[1] == "腾讯 控股"
        assert res[2] == "中国"
        conn.close()


def test_backslash_escaped_unicode_in_json():
    """JSON containing unicode escape sequences (e.g. \\u00e9 for é)."""
    j_str = '{"name": "Cr\\u00e9dit Agricole"}'
    data = json.loads(j_str)
    assert data["name"] == "Crédit Agricole"


def test_parquet_dictionary_encoding_with_unicode_strings(tmp_path: Path):
    """Parquet dictionary encoding retains unique Unicode categories accurately."""
    fpath = tmp_path / "dict_unicode.parquet"
    categories = ["USD", "EUR €", "JPY ¥", "GBP £", "CNY ¥"]
    data = categories * 20  # 100 rows

    dict_arr = pa.DictionaryArray.from_arrays(
        pa.array([i % 5 for i in range(100)], type=pa.int8()),
        pa.array(categories, type=pa.string()),
    )
    table = pa.table({"currency": dict_arr})

    pq.write_table(table, fpath)
    res = pq.read_table(fpath)
    assert res["currency"].to_pylist() == data
