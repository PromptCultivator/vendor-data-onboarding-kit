# ADR 005: Model-Free Rule-Based Identifier Mapping

## Status
Accepted

## Context
Financial market entities undergo corporate reorganizations, ticker reassignments, stock splits, and name changes. Data providers use differing identifier standards: CIK numbers in SEC filings, ticker symbols in market data feeds, CUSIPs or ISINs in custody reports, and vendor-specific proprietary codes.

Resolving diverse input symbols to a canonical enterprise identifier is required for cross-dataset quantitative analysis. However, automated identifier resolution carries significant risks:
- If a resolution engine relies on fuzzy text matching or probabilistic machine-learning models, it can silently map an entity to the wrong company (e.g., matching a newly created shell company to an established corporation with a similar name).
- Erroneous entity mappings propagate silently into quantitative trading signals, causing incorrect position sizing and unexpected trading losses.

## Options Considered

1. **Probabilistic / Fuzzy Entity Matching (Levenshtein, Jaro-Winkler, LLM Matching)**:
   - Match unresolved entity names or tickers to the closest known master record using similarity metrics.
   - *Pros*: High automated match rate; reduces manual resolution effort.
   - *Cons*: Non-deterministic behavior; false-positive joins introduce silent data corruption that is hard to audit in production.

2. **Hard Abort on Unmapped Symbols**:
   - Abort pipeline execution whenever an unfamiliar identifier is encountered.
   - *Pros*: Zero risk of unmapped entities entering storage.
   - *Cons*: High operational fragility; an unmapped minor firm in a 5,000-stock universe blocks data ingestion for the entire market.

3. **Deterministic Rule-Based Resolution with Unmapped Flagging**:
   - Resolve identifiers using exact rule-based matching against a versioned symbol master (e.g., SEC `company_tickers.json`) with documented exchange suffix stripping.
   - For unresolved entities, retain the raw identifier, assign a deterministic synthetic key, flag `unmapped = true`, and report the occurrences in the readiness scorecard. Never guess.

## Decision
Adopt **Option 3 (Deterministic Rule-Based Resolution with Unmapped Flagging)**.

- All identifier mapping rules are deterministic and auditable.
- Ticker suffixes (such as `.O`, `.N`, `.P`) are stripped according to a documented lookup table.
- If an entity cannot be matched, it is marked `unmapped = true` and continues through ingestion with its raw identifier preserved.
- The readiness report tallies unmapped entity counts and lists the most frequent unmapped identifiers, giving data operations teams a clear queue for investigation.

## Consequences

- **Optimizes**: Auditability, deterministic reproducibility, and elimination of false-positive entity joins in quantitative models.
- **Costs**:
  - Manual review queue: Unresolved identifiers require human verification or vendor clarification to add to the master.
  - Coverage gap reporting: Datasets with unfamiliar vendor codes exhibit lower initial mapping rates until mapping rules are updated.
