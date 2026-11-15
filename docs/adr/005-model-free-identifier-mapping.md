# ADR 005: Model-Free Rule-Based Identifier Mapping

## Status
Accepted

## Context
Financial market entities undergo corporate reorganizations, ticker reassignments, stock splits, and name changes. Data providers use differing identifier standards: CIK numbers in SEC filings, ticker symbols in market feeds, CUSIPs or ISINs in custody reports, and vendor-specific codes.

Resolving diverse symbols to a canonical identifier is required for cross-dataset analysis, but probabilistic or fuzzy matching carries severe integrity risks: silently mapping an unknown entity to the wrong company (such as a newly formed entity with a similar name) corrupts downstream models without raising an error.

## Options Considered

1. **Probabilistic or Fuzzy Entity Matching (Edit Distance, ML Classifiers)**:
   - Map unresolved entity names or tickers to the closest master entry using string similarity or statistical models.
   - *Pros*: Higher automated match rate; smaller manual review queue.
   - *Cons*: Non-auditable or false-positive matches silently corrupt entity joins across quantitative signals.

2. **Hard Abort on Unmapped Symbols**:
   - Halt ingestion whenever any unrecognized identifier appears.
   - *Pros*: Zero unmapped entities in storage.
   - *Cons*: Fragile in production; a single unfamiliar or delisted issuer blocks an entire market feed.

3. **Deterministic Rule-Based Resolution with Unmapped Flagging**:
   - Resolve identifiers via deterministic rules against a content-hash-versioned symbol master (`company_tickers.json`) with documented exchange suffix stripping and dual-class normalization.
   - Retain normalized raw identifiers for unresolved rows, flag `unmapped = true`, and surface the unmapped queue in the readiness report.

## Decision
Adopt **Option 3 (Deterministic Rule-Based Resolution with Unmapped Flagging)**.

- All identifier mapping rules are deterministic and explicit: 10-digit zero-padded CIK matching, uppercase ticker matching with documented suffix stripping (`.OQ`, `.N`, `.P`, etc.) and dual-class separator normalization (`.`/`/` → `-`), and exact normalized title matching.
- Unresolved entities remain in ingestion flagged `unmapped = true` with their normalized raw identifier preserved.
- The readiness report computes overall mapped share and lists the most frequent unmapped identifiers for operator review.

## Consequences

- **Optimizes**: Auditability, deterministic reproducibility, and elimination of false-positive entity joins.
- **Costs**:
  - Manual review queue: Unresolved identifiers require human verification or symbol master updates. On SEC FSDS `2023q3`, `71.90%` of records (`2,458,042`) resolved against the active SEC exchange ticker master while `28.10%` (`960,840` records from non-exchange-traded filers, funds, and delisted entities) entered the reported unmapped queue.
  - Lower initial coverage on proprietary feeds until explicit mapping entries are added to the symbol master.
