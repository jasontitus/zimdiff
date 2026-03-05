# zimdiff

Overlay-based incremental updates for [ZIM files](https://openzim.org). Generate compact diffs between ZIM file versions and apply them without needing double the storage.

## The Problem

ZIM files like `wikipedia_en_all_maxi` are 115+ GB. Updating to a new version means downloading the whole thing again. On mobile devices with limited storage, you can't even hold two copies at once.

## The Solution

**Overlay ZIMs** — a diff that is itself a valid ZIM file containing only changed/added entries plus a deletion list. On the client:

1. **Download the overlay** (~8% of the full file for a 3-month Wikipedia update)
2. **Use immediately** — the `OverlayReader` resolves entries across base + overlay layers
3. **Flatten later** — when storage allows, merge base + overlays into a single updated ZIM

```
Full Wikipedia update:     115 GB download, 230 GB peak storage
With zimdiff overlay:      ~8 GB download, ~123 GB peak storage (no flatten needed)
                          ~8 GB download, ~230 GB peak storage (with flatten)
```

## Usage

### Generate an overlay (server-side)

```bash
zimdiff diff old.zim new.zim -o overlay.zim
```

### Inspect an overlay

```bash
zimdiff info overlay.zim
```

### Verify correctness

```bash
zimdiff verify old.zim overlay.zim --reference new.zim
```

### Flatten base + overlays (client-side, when storage allows)

```bash
zimdiff apply base.zim overlay1.zim overlay2.zim -o updated.zim
```

### Use overlays programmatically

```python
from zimdiff import OverlayReader

reader = OverlayReader("base.zim", ["overlay1.zim", "overlay2.zim"])

# Resolve entries across all layers (newest overlay wins)
result = reader.get_entry("United_States")
if result:
    archive, entry = result
    print(entry.get_item().content)

# Iterate all live entries
for path in reader.iter_paths():
    print(path)
```

## How It Works

1. **diff** compares two ZIM files entry-by-entry using [libzim](https://github.com/openzim/python-libzim), identifying added, modified, and removed entries
2. The overlay is written as a standard ZIM file with extra metadata:
   - `zimdiff.deleted` — JSON list of removed paths
   - `zimdiff.base_uuid` — UUID of the base ZIM it applies to
   - `zimdiff.version` — format version for compatibility
3. `OverlayReader` checks overlays newest-first, then falls back to the base. Deleted entries are filtered out.
4. Multiple overlays can stack (e.g., monthly updates without ever flattening)

## Install

```bash
python -m venv .venv
source .venv/bin/activate
pip install libzim
```

## Test Results

With `wikipedia_en_100` (top 100 articles, Oct 2025 → Jan 2026):

| Metric | Value |
|--------|-------|
| Old ZIM | 313 MB |
| New ZIM | 319 MB |
| **Overlay** | **24.6 MB (8%)** |
| Changed entries | 654 of 8,950 |
| Content mismatches | 0 |

## Compared to Other Approaches

| Approach | Diff Size | RAM Needed | Mobile Viable? |
|----------|-----------|------------|----------------|
| **zimdiff overlay** | **24.6 MB** | **~1 MB** | **Yes** |
| bsdiff | 24 MB | ~17x file size | No (needs TB of RAM) |
| xdelta3 | 240 MB | Bounded | Yes, but diffs are huge |
| Official zimdiff | 25 MB | ~1 MB | Partial (C++ only) |

## License

MIT
