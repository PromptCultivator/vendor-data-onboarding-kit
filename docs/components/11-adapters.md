# Component: Ingestion Adapters (`vdok.adapters`)

## 1. Overview & Responsibility

The `vdok.adapters` package contains specialized ingestion connectors that translate heterogeneous external formats into the canonical normalized model. While the core engine operates format-agnostically via vendor manifests, specialized adapters handle complex domain relationships, multi-file relational joins, and automated fault generation.

Three adapters are provided:
1. `sec_fsds.py`: Production adapter for SEC Financial Statement Data Sets.
2. `synthetic.py`: Configurable synthetic drop generator with planted data anomalies.
3. `alfred.py`: Macroeconomic vintage time-series adapter for Federal Reserve ALFRED data.

---

## 2. SEC Financial Statement Data Sets Adapter (`sec_fsds.py`)

### 2.1 Dataset Structure & Acquisition
The SEC publishes quarterly zip archives containing four tab-delimited relational tables:
- `sub.txt`: Submissions table containing filing metadata (`adsh`, `cik`, `filed`, `form`, `period`).
- `num.txt`: Numeric data points table (`adsh`, `tag`, `version`, `ddate`, `qtrs`, `uom`, `value`, `coreg`).
- `tag.txt`: Taxonomy tag definitions and accounting labels.
- `pre.txt`: Statement presentation hierarchy.

Files are acquired via `make data` with compliant rate-limiting ($\le 5\text{ req/sec}$) and an explicit user agent declaration (`User-Agent: vendor-data-onboarding-kit <email>`).

### 2.2 Relational Joining & Field Mapping
The adapter executes an inner join between `num` and `sub` on Accession Number (`adsh`):

```sql
SELECT
    LPAD(s.cik::VARCHAR, 10, '0') || COALESCE('|coreg=' || n.coreg, '') AS entity_id,
    'cik' AS entity_id_type,
    CONCAT_WS('|', n.tag, n.uom, n.qtrs::VARCHAR) AS field,
    strptime(n.ddate::VARCHAR, '%Y%m%d')::TIMESTAMPTZ AS event_time,
    strptime(s.filed::VARCHAR, '%Y%m%d')::TIMESTAMPTZ AS knowledge_time,
    n.value::DECIMAL(38, 9) AS value_num,
    NULL AS value_str,
    n.uom AS unit,
    ? AS drop_id,
    'num.txt' AS source_file,
    row_number() OVER () AS source_row
FROM read_csv_auto('num.txt', delim='\t') n
JOIN read_csv_auto('sub.txt', delim='\t') s
  ON n.adsh = s.adsh;
```

### 2.3 Real Restatement Ground Truth
The SEC FSDS serves as the primary evaluation dataset because public companies frequently file amended reports (Form 10-K/A or 10-Q/A) or restate prior-period numbers in subsequent annual filings. The identical logical key `(cik, tag, ddate, qtrs)` appears across multiple quarters with differing values and advancing filing dates (`filed`), providing authentic ground-truth restatements.

---

## 3. Synthetic Benchmark Generator (`synthetic.py`)

To evaluate pipeline throughput, memory limits, and revision detection accuracy under controlled conditions, `vdok.adapters.synthetic` procedurally generates multi-drop synthetic test suites.

### 3.1 Planted Fault Architecture
The generator can inject nine distinct fault types, each individually configurable:

| Fault Type | Category | Injected Characteristic | Expected System Handling |
|---|---|---|---|
| `restatement` | Revisions | Altered values for identical prior keys with advancing knowledge time | 100% detected by Revision Ledger |
| `duplicate_row` | Validation | Repeated rows sharing identical primary keys | Quarantined or Batch Held |
| `type_flip` | Schema | String values injected into numeric columns | Quarantined / Type violation |
| `out_of_range` | Validation | Negative prices or share volumes | Quarantined by Range rule |
| `missing_period` | Coverage | Complete removal of an expected calendar period | Flagged in Coverage Matrix |
| `identifier_change`| Symbology | Entity ID modified to an alias present in symbol master | Resolved via Symbology Master |
| `unit_change` | Validation | Value shifted from USD to thousands of USD | Flagged by unit validation |
| `late_row` | Validation | Event time far preceding knowledge time | Handled or flagged by latency check |
| `repeat_drop` | Batch | Byte-identical transmission of the previous drop | Ingestion aborted as duplicate |
| `schema_drift` | Schema | Removal of a required column in drop 3 | Batch Held by Schema Guard |

### 3.2 CLI Generation
```bash
vdok synth --size-gb 1 --seed 1 --faults default
```

---

## 4. St. Louis Fed FRED/ALFRED Vintage Adapter (`alfred.py`)

ALFRED provides historical macroeconomic time series along with their respective observation vintages.

### 4.1 Temporal Mapping
- Economic observation period `date` maps to `event_time`.
- Vintage release date `realtime_start` maps to `knowledge_time`.

### 4.2 Look-Ahead Bias Prevention
Macroeconomic announcements (such as Nonfarm Payrolls or GDP) undergo substantial revision cycles across Advance, Preliminary, and Final estimates. The ALFRED adapter preserves every vintage, guaranteeing that backtests evaluated on historical FOMC meeting dates observe only the data vintage that was accessible on that specific trading day.
