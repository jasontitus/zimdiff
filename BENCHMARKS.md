# Benchmarks

## Test Files

### Small: `wikipedia_en_100` (top 100 articles)

| File | Version | Size | Entries |
|------|---------|------|---------|
| `wikipedia_en_100_2025-10.zim` | Oct 2025 | 313 MB | 8,950 |
| `wikipedia_en_100_2026-01.zim` | Jan 2026 | 319 MB | ~8,950 |

### Full: `wikipedia_en_all_maxi` (complete English Wikipedia)

| File | Version | Size | Entries | UUID |
|------|---------|------|---------|------|
| `wikipedia_en_all_maxi_2025-08.zim` | Aug 2025 | 111.1 GB | 26,659,463 | `fc5f2d86-babf-5e64-7166-cf84aea2362e` |
| `wikipedia_en_all_maxi_2026-02.zim` | Feb 2026 | 115.5 GB | 27,199,904 | `53a8f711-f7f9-ec23-08fc-38fcf579ba32` |

Files sourced from [download.kiwix.org](https://download.kiwix.org/zim/wikipedia/).

---

## Small File Results (3-month gap)

```
zimdiff diff wikipedia_en_100_2025-10.zim wikipedia_en_100_2026-01.zim -o overlay.zim
```

| Metric | Value |
|--------|-------|
| Old ZIM | 313 MB |
| New ZIM | 319 MB |
| **Overlay** | **24.6 MB (7.7%)** |
| Changed entries | 654 of 8,950 |
| Content mismatches | 0 |

### Comparison with other diff tools (small files)

| Approach | Diff Size | RAM Needed | Mobile Viable? |
|----------|-----------|------------|----------------|
| **zimdiff overlay** | **24.6 MB** | **~1 MB** | **Yes** |
| bsdiff | 24 MB | ~17x file size | No (needs TB of RAM for large files) |
| xdelta3 | 240 MB | Bounded | Yes, but diffs are huge |
| Official zimdiff | 25 MB | ~1 MB | Partial (C++ only) |

bsdiff produces nearly identical diff sizes, confirming that zimdiff's content-level approach captures real changes accurately. xdelta3 performs poorly because ZIM's zstd compression defeats byte-level delta encoding.

---

## Full Wikipedia Results (6-month gap)

```
zimdiff diff \
  wikipedia_en_all_maxi_2025-08.zim \
  wikipedia_en_all_maxi_2026-02.zim \
  -o overlay_feb2026.zim \
  --skip-mime "application/octet-stream+xapian" \
  --skip-mime "application/octet-stream+zimlisting"
```

### Summary

| Metric | Value |
|--------|-------|
| Old ZIM | 111.1 GB (26,659,463 entries) |
| New ZIM | 115.5 GB (27,199,904 entries) |
| **Overlay** | **105.1 GB (91.0% of full)** |
| Overlay entries | 15,820,731 |
| Runtime | ~5 hours |
| Peak RSS | 54.3 GB |

### Change Breakdown

| Category | Count | Notes |
|----------|-------|-------|
| Added entries | 8,559,175 | New in Feb 2026, not in Aug 2025 |
| Removed entries | 8,018,734 | In Aug 2025, not in Feb 2026 |
| Common entries | 18,640,729 | Present in both versions |
| Modified (redirect/title) | 161,141 | Redirect target or title changed |
| MIME type changed | 11 | Entry changed content type |
| Content modified | 7,261,556 | Same path, different content |
| Unchanged | 11,379,173 | Identical content |

### Why Is the Overlay So Large?

For a 6-month gap, the overlay is 91% of the full file -- essentially no savings. Three factors drive this:

1. **Massive path churn (8.5M added / 8M removed):** Wikipedia ZIM dumps reorganize media and image paths between versions. Even if the underlying image data is identical, a changed path means the entry appears as "added" (new path) and "removed" (old path). This alone accounts for ~8.5M entries in the overlay.

2. **HTML re-rendering (7.3M of 18.6M common entries modified):** ZIM files store rendered HTML, not wikitext source. When Wikipedia updates its templates, CSS, or rendering pipeline, every article's HTML changes -- even if the article content itself is untouched. Over a 6-month gap, this affects 39% of common entries.

3. **Search index excluded but still large churn:** The `--skip-mime` flag excluded the ~13 GB Xapian search index, but the content churn alone is overwhelming.

### Implications

| Update Interval | Estimated Overlay | % of Full | Viable? |
|-----------------|-------------------|-----------|---------|
| 6 months | ~105 GB | ~91% | No -- just download the full file |
| 3 months | ~8-30 GB (est.) | ~7-26% | Marginal -- depends on template changes |
| 1 month | ~3-8 GB (est.) | ~3-7% | Yes -- sweet spot for overlays |

The overlay approach is most effective with **frequent, short-interval updates** (monthly or more often). For long gaps, the cumulative churn of path reorganization and HTML re-rendering makes the overlay nearly as large as the full file.

### Performance

| Phase | Duration | Rate |
|-------|----------|------|
| Index old entries | ~84s | ~317K entries/s |
| Index new entries | ~84s | ~317K entries/s |
| Set operations | ~5s | instant |
| Redirect/title comparison | ~25s | ~745K entries/s |
| MIME type parsing (binary) | ~23s | ~1.4M entries/s |
| Content hashing (old) | ~19 min | ~7,200 items/s |
| Content hashing (new) | ~19 min | ~7,200 items/s |
| Write redirects | ~2s | ~150K entries/s |
| Write items | ~5 hours | ~855 entries/s |
| **Total** | **~5 hours** | |

Content hashing uses direct mmap + zstandard cluster decompression, bypassing libzim's Python bindings which are ~140x slower (~40 items/s vs ~7,200 items/s).

---

## Search Index Analysis

See [SEARCH_INDEX.md](SEARCH_INDEX.md) for detailed analysis of the Xapian search index (~12-13 GB, 11% of total) and strategies for handling it in overlays.
