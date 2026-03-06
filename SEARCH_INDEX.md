# Search Index Strategy for ZIM Overlays

## The Problem

ZIM files include a full-text search index (Xapian) that is 11-12% of the total file size:

| Component | Old (Aug 2025) | New (Feb 2026) |
|-----------|---------------|----------------|
| Full-text index (`fulltext/xapian`) | 7.47 GB | 7.66 GB |
| Title index (`title/xapian`) | 3.16 GB | 3.22 GB |
| Title listing (`listing/titleOrdered/v1`) | 1.76 GB | 2.06 GB |
| **Total search index** | **12.40 GB** | **12.93 GB** |
| Non-index content | 98.68 GB | 102.54 GB |
| **Total ZIM** | **111.1 GB** | **115.5 GB** |

The search index is a monolithic Xapian B-tree database. It changes almost completely between versions because:
- Internal document IDs shift when articles are added/removed
- B-tree structure is rebuilt from scratch each dump
- Even unchanged articles get new positions in the index

Including the search index in the overlay adds ~13 GB regardless of how few articles actually changed.

## Overlay Size: Actual Results (6-month gap, full English Wikipedia)

Tested with `wikipedia_en_all_maxi_2025-08.zim` (111.1 GB) → `wikipedia_en_all_maxi_2026-02.zim` (115.5 GB):

| Scenario | Overlay Entries | Size | % of Full |
|----------|-----------------|------|-----------|
| **Without search index** (actual) | **15,820,731** | **105.1 GB** | **91%** |
| 1-month gap (estimated) | ~2-3M | ~3-8 GB | ~3-7% |

The 6-month overlay is 91% of the full file due to massive path churn (8.5M added/8M removed) and HTML re-rendering (39% of common entries modified). The search index exclusion had minimal impact because Xapian entries are stored in clusters not accessible via `has_entry_by_path()` and were auto-excluded. See [BENCHMARKS.md](BENCHMARKS.md) for the full breakdown.

## Options

### 1. Exclude Search Index from Overlay (Recommended for MVP)

**How:** Add `--skip-mime` flag to exclude `application/octet-stream+xapian` and `application/octet-stream+zimlisting` entries from the overlay.

**Client experience:**
- Search uses the base version's index (slightly stale)
- All article content is current via the overlay
- No CPU cost on device
- Saves ~13 GB download

**Tradeoffs:**
- Search results may reference deleted articles (filtered by OverlayReader)
- New articles won't appear in search until flatten
- Title search may miss renamed articles

**Best for:** Bandwidth-constrained updates where stale search is acceptable.

### 2. Ship Full New Search Index as Separate Download

**How:** Extract the 3 search index entries as a standalone overlay ZIM. Client downloads content overlay + search overlay separately.

**Client experience:**
- Content overlay: ~20-35 GB (articles and media)
- Search overlay: ~13 GB (optional, download when on WiFi)
- Full search works immediately when search overlay is applied

**Tradeoffs:**
- Total download is similar to including it
- But search download is optional and deferrable
- User can prioritize content freshness over search freshness

### 3. Incremental Search Index via Xapian Replication

**How:** Use Xapian's built-in replication protocol which supports incremental updates to the B-tree.

**Client experience:**
- Small incremental search updates (~100 MB estimated)
- Full search always up-to-date

**Tradeoffs:**
- Requires Xapian replication server infrastructure
- Client needs Xapian native library (not just the ZIM reader)
- Complex implementation
- Would need to extract Xapian DB from ZIM, apply replication, re-embed

### 4. Client-Side Search Index Rebuild

**How:** After applying the content overlay, rebuild the search index on device.

**Client experience:**
- Download small content overlay only
- Background index rebuild

**Tradeoffs:**
- CPU-intensive: 2-8 hours on modern phone for full Wikipedia
- Battery drain during rebuild
- Storage spike during rebuild (~2x index size temporarily)
- Requires Xapian write support on device

### 5. Server-Side Search API

**How:** Search queries go to a remote API when connectivity is available. Offline search uses the base (stale) index.

**Client experience:**
- Online: always current search results
- Offline: stale search results from base index
- Zero additional download for search

**Tradeoffs:**
- Requires server infrastructure and ongoing costs
- Defeats purpose of offline-first ZIM approach
- Not viable for truly offline use cases

## Recommendation

**Short term:** Option 1 (exclude search index) — simplest, saves 13 GB, stale search is acceptable since article content is current.

**Medium term:** Option 2 (separate search overlay) — gives users the choice to download search separately.

**Long term:** Option 3 (Xapian incremental replication) — most efficient but highest complexity.

## Implementation Plan

Adding `--skip-mime` flag to `zimdiff diff`:
```bash
# Exclude search index from overlay
zimdiff diff old.zim new.zim -o overlay.zim --skip-mime "application/octet-stream+xapian" --skip-mime "application/octet-stream+zimlisting"

# Generate search-only overlay separately
zimdiff diff old.zim new.zim -o search_overlay.zim --only-mime "application/octet-stream+xapian" --only-mime "application/octet-stream+zimlisting"
```
