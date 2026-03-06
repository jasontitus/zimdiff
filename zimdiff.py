#!/usr/bin/env python3
"""zimdiff - Overlay-based incremental updates for ZIM files.

Server-side:
    zimdiff diff <old.zim> <new.zim> -o <overlay.zim>
        Compare two ZIM files and produce a compact overlay ZIM containing
        only added/modified entries plus a deletion list.

Client-side:
    zimdiff apply <base.zim> <overlay1.zim> [overlay2.zim ...] -o <output.zim>
        Flatten base + overlays into a single updated ZIM.
        Requires enough storage for the output file.

Info:
    zimdiff info <overlay.zim>
        Show what's in an overlay.

    zimdiff verify <base.zim> <overlay.zim> --reference <new.zim>
        Verify that base + overlay produces the same content as reference.
"""

import argparse
import hashlib
import json
import mmap
import os
import resource
import struct
import sys
import time

import zstandard
from tqdm import tqdm

from libzim.reader import Archive
from libzim.writer import Creator, Item, StringProvider, Hint


OVERLAY_VERSION = 2
DELETION_META_KEY = "zimdiff.deleted"
BASE_UUID_META_KEY = "zimdiff.base_uuid"
BASE_CHECKSUM_META_KEY = "zimdiff.base_checksum"
TARGET_UUID_META_KEY = "zimdiff.target_uuid"
VERSION_META_KEY = "zimdiff.version"
PARENT_UUID_META_KEY = "zimdiff.parent_uuid"


def _fmt(num_bytes):
    """Format byte count as human-readable string."""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(num_bytes) < 1024:
            return f"{num_bytes:.1f} {unit}"
        num_bytes /= 1024
    return f"{num_bytes:.1f} PB"


def _rss():
    """Current RSS in bytes."""
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss


def _rss_mb():
    """Current RSS as human string. macOS reports bytes, Linux reports KB."""
    raw = _rss()
    if sys.platform == "darwin":
        return _fmt(raw)
    return _fmt(raw * 1024)


def _estimate_set_mem(s):
    """Rough memory estimate for a set of strings."""
    if not s:
        return 0
    # set overhead + per-element pointer + string objects
    # Average string length approximation from sample
    sample = list(s)[:1000]
    avg_len = sum(len(x) for x in sample) / len(sample)
    # CPython string: 49 bytes base + 1 byte per char (ASCII)
    per_str = 49 + avg_len
    # set: ~72 bytes overhead + 8 bytes per bucket (load factor ~0.66)
    return int(72 + len(s) * (8 / 0.66) + len(s) * per_str)


def _parse_zim_entries(filepath):
    """Parse ZIM binary for MIME types and per-entry metadata without decompression.

    Returns (mime_list, url_ptrs, entry_meta) where:
        mime_list: list of MIME type strings
        url_ptrs: tuple of directory entry offsets
        entry_meta: dict {entry_id: (mime_str, cluster_num, blob_num)} for items only
    """
    with open(filepath, 'rb') as f:
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)

        entry_count = struct.unpack_from('<I', mm, 24)[0]
        url_ptr_pos = struct.unpack_from('<Q', mm, 32)[0]
        mime_list_pos = struct.unpack_from('<Q', mm, 56)[0]

        # Parse MIME type list (null-terminated strings, empty string ends list)
        mime_list = []
        pos = mime_list_pos
        while True:
            end = mm.find(b'\x00', pos)
            s = mm[pos:end].decode('utf-8')
            if s == '':
                break
            mime_list.append(s)
            pos = end + 1

        # Read all URL pointers
        url_ptrs = struct.unpack_from(f'<{entry_count}Q', mm, url_ptr_pos)

        # Parse entry metadata: mime_idx, cluster, blob for items
        entry_meta = {}
        for i in range(entry_count):
            offset = url_ptrs[i]
            mime_idx = struct.unpack_from('<H', mm, offset)[0]
            if mime_idx != 0xFFFF:
                cluster_num = struct.unpack_from('<I', mm, offset + 8)[0]
                blob_num = struct.unpack_from('<I', mm, offset + 12)[0]
                entry_meta[i] = (mime_list[mime_idx], cluster_num, blob_num)

        mm.close()

    return mime_list, url_ptrs, entry_meta


def _hash_items_direct(filepath, items_to_hash, label):
    """Hash item content by reading clusters directly from the ZIM binary.

    Bypasses libzim for ~140x faster cluster decompression.

    Args:
        filepath: path to ZIM file
        items_to_hash: dict {path: (entry_id, cluster_num, blob_num)}
        label: label for progress bar

    Returns:
        dict {path: md5_digest}
    """
    if not items_to_hash:
        return {}

    # Group items by cluster number
    cluster_items = {}  # cluster_num -> [(path, blob_num)]
    for path, (eid, cnum, bnum) in items_to_hash.items():
        cluster_items.setdefault(cnum, []).append((path, bnum))

    with open(filepath, 'rb') as f:
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)

        cluster_count = struct.unpack_from('<I', mm, 28)[0]
        cluster_ptr_pos = struct.unpack_from('<Q', mm, 48)[0]
        checksum_pos = struct.unpack_from('<Q', mm, 72)[0]

        cluster_ptrs = list(struct.unpack_from(f'<{cluster_count}Q', mm, cluster_ptr_pos))
        cluster_ptrs.append(checksum_pos)

        dctx = zstandard.ZstdDecompressor()
        hashes = {}
        total_items = len(items_to_hash)
        skipped_large = 0

        with tqdm(total=total_items, desc=f"Hash {label}", unit=" items", miniters=1000,
                  bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]") as pbar:
            for cnum in sorted(cluster_items.keys()):
                blobs = cluster_items[cnum]
                start = cluster_ptrs[cnum]
                end = cluster_ptrs[cnum + 1]
                comp_size = end - start

                # For very large clusters (>100MB), use streaming decompression
                if comp_size > 100 * 1024 * 1024:
                    # These are typically single-blob clusters (e.g. Xapian index)
                    # Stream-hash them to avoid huge memory allocation
                    comp_type = mm[start]
                    if comp_type in (4, 5) and len(blobs) == 1:
                        path, bnum = blobs[0]
                        hasher = hashlib.md5()
                        reader = dctx.stream_reader(bytes(mm[start + 1:end]))
                        while True:
                            chunk = reader.read(4 * 1024 * 1024)
                            if not chunk:
                                break
                            hasher.update(chunk)
                        hashes[path] = hasher.digest()
                        pbar.update(1)
                    else:
                        skipped_large += len(blobs)
                        pbar.update(len(blobs))
                    continue

                # Read and decompress cluster
                comp_type = mm[start]
                compressed = bytes(mm[start + 1:end])

                if comp_type in (4, 5):  # zstd
                    try:
                        decompressed = dctx.decompress(compressed, max_output_size=max(len(compressed) * 30, 64 * 1024 * 1024))
                    except zstandard.ZstdError:
                        # Fallback: streaming decompression for tricky frames
                        chunks = []
                        reader = dctx.stream_reader(compressed)
                        while True:
                            chunk = reader.read(1024 * 1024)
                            if not chunk:
                                break
                            chunks.append(chunk)
                        decompressed = b''.join(chunks)
                elif comp_type == 1:  # uncompressed
                    decompressed = compressed
                else:
                    skipped_large += len(blobs)
                    pbar.update(len(blobs))
                    continue

                # Parse blob offset table
                first_offset = struct.unpack_from('<I', decompressed, 0)[0]
                n_blobs = first_offset // 4
                if n_blobs > 0:
                    offsets = struct.unpack_from(f'<{n_blobs}I', decompressed, 0)
                else:
                    pbar.update(len(blobs))
                    continue

                for path, bnum in blobs:
                    if bnum >= n_blobs:
                        pbar.update(1)
                        continue
                    blob_start = offsets[bnum]
                    blob_end = offsets[bnum + 1] if bnum + 1 < n_blobs else len(decompressed)
                    content = decompressed[blob_start:blob_end]
                    hashes[path] = hashlib.md5(content).digest()
                    pbar.update(1)

        mm.close()

    if skipped_large:
        print(f"  Skipped {skipped_large} items in unsupported clusters")

    return hashes


class ZimItem(Item):
    def __init__(self, path, title, mimetype, content):
        super().__init__()
        self._path = path
        self._title = title
        self._mimetype = mimetype
        self._content = content if isinstance(content, bytes) else content.encode("utf-8")

    def get_path(self):
        return self._path

    def get_title(self):
        return self._title

    def get_mimetype(self):
        return self._mimetype

    def get_contentprovider(self):
        return StringProvider(self._content)

    def get_hints(self):
        return {Hint.FRONT_ARTICLE: self._mimetype == "text/html"}


def _iter_entries(archive):
    """Yield (path, entry) for all user-accessible entries."""
    for i in range(archive.all_entry_count):
        entry = archive._get_entry_by_id(i)
        if archive.has_entry_by_path(entry.path):
            yield entry.path, entry


def _index_archive(archive, label):
    """Build a {path: info_tuple} index with lightweight metadata.

    Fast pass (~350K entries/s): captures path, title, redirect info,
    and entry ID (for sequential access later).
    Avoids get_item() which triggers expensive cluster decompression.

    Info tuples:
        Redirects: (True, title, target_path, entry_id)
        Items:     (False, title, None, entry_id)
    """
    total = archive.all_entry_count
    index = {}
    with tqdm(range(total), desc=f"Indexing {label}", unit=" entries",
              bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]") as pbar:
        for i in pbar:
            entry = archive._get_entry_by_id(i)
            if not archive.has_entry_by_path(entry.path):
                continue
            if entry.is_redirect:
                index[entry.path] = (True, entry.title, entry.get_redirect_entry().path, i)
            else:
                index[entry.path] = (False, entry.title, None, i)
    mem_est = _estimate_set_mem(set(index.keys()))
    print(f"  {len(index):,} entries indexed, ~{_fmt(mem_est)} path memory, RSS {_rss_mb()}")
    return index


def _resolve_main_entry(archive):
    """Follow redirects to find the main entry's content path."""
    if not archive.has_main_entry:
        return None
    entry = archive.main_entry
    while entry.is_redirect:
        entry = entry.get_redirect_entry()
    return entry.path


def _read_overlay_metadata(archive):
    """Read zimdiff overlay metadata from a ZIM file. Returns None if not an overlay."""
    try:
        version = int(archive.get_metadata(VERSION_META_KEY))
    except Exception:
        return None
    deleted_json = archive.get_metadata(DELETION_META_KEY).decode("utf-8")
    return {
        "version": version,
        "base_uuid": archive.get_metadata(BASE_UUID_META_KEY).decode("utf-8"),
        "base_checksum": archive.get_metadata(BASE_CHECKSUM_META_KEY).decode("utf-8"),
        "target_uuid": archive.get_metadata(TARGET_UUID_META_KEY).decode("utf-8"),
        "parent_uuid": archive.get_metadata(PARENT_UUID_META_KEY).decode("utf-8"),
        "deleted": json.loads(deleted_json),
    }


# ---------------------------------------------------------------------------
# Overlay reader
# ---------------------------------------------------------------------------


class OverlayReader:
    """Read from a base ZIM with overlay ZIMs stacked on top.

    Entry resolution order: last overlay -> ... -> first overlay -> base.
    """

    def __init__(self, base_path, overlay_paths=None):
        self.base = Archive(base_path)
        self.overlays = []
        self._deleted_sets = []
        for path in overlay_paths or []:
            archive = Archive(path)
            self.overlays.append(archive)
            meta = _read_overlay_metadata(archive)
            self._deleted_sets.append(set(meta["deleted"]) if meta else set())

    def get_entry(self, path):
        """Return (archive, entry) for a path, or None if deleted/missing."""
        for i in range(len(self.overlays) - 1, -1, -1):
            overlay = self.overlays[i]
            if overlay.has_entry_by_path(path):
                return overlay, overlay.get_entry_by_path(path)
            if path in self._deleted_sets[i]:
                return None
        if self.base.has_entry_by_path(path):
            return self.base, self.base.get_entry_by_path(path)
        return None

    def iter_paths(self):
        """Yield all live paths across base + overlays."""
        all_deleted = set()
        overlay_paths = set()

        for i in range(len(self.overlays) - 1, -1, -1):
            for path, _ in _iter_entries(self.overlays[i]):
                overlay_paths.add(path)
            for path in self._deleted_sets[i]:
                if path not in overlay_paths:
                    all_deleted.add(path)

        yield from overlay_paths

        for path, _ in _iter_entries(self.base):
            if path not in overlay_paths and path not in all_deleted:
                yield path


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------


def cmd_diff(args):
    """Generate an overlay ZIM from old and new ZIM files."""
    old = Archive(args.old)
    new = Archive(args.new)

    print(f"Old: {old.entry_count:,} entries ({_fmt(old.filesize)}) [{old.uuid}]")
    print(f"New: {new.entry_count:,} entries ({_fmt(new.filesize)}) [{new.uuid}]")
    print(f"Initial RSS: {_rss_mb()}")
    print()

    # Phase 1-2: Index both archives with full metadata
    # Captures (is_redirect, title, mimetype/target, size) per entry during
    # sequential scan so the comparison phase needs zero archive lookups.
    old_index = _index_archive(old, "old")
    new_index = _index_archive(new, "new")

    old_paths = set(old_index.keys())
    new_paths = set(new_index.keys())
    added_keys = sorted(new_paths - old_paths)
    removed_keys = sorted(old_paths - new_paths)
    common_keys = sorted(old_paths & new_paths)

    print(f"\n  Added:  {len(added_keys):,}  Removed: {len(removed_keys):,}  Common: {len(common_keys):,}")
    mem_total = _estimate_set_mem(old_paths) + _estimate_set_mem(new_paths)
    print(f"  Path index memory: ~{_fmt(mem_total)}, RSS: {_rss_mb()}")

    # Phase 3a: Compare redirects in-memory (instant, no I/O)
    modified_keys = []
    common_items = []  # items present in both — need deeper comparison
    skipped_by_meta = 0

    print(f"\nPhase 3a: Comparing {len(common_keys):,} common entries (redirect/title check)...")
    t0 = time.time()
    for path in common_keys:
        old_info = old_index[path]
        new_info = new_index[path]

        # Different type (redirect vs item) or different title
        if old_info[0] != new_info[0] or old_info[1] != new_info[1]:
            modified_keys.append(path)
            skipped_by_meta += 1
            continue

        # Both redirects — compare targets
        if old_info[0]:
            if old_info[2] != new_info[2]:
                modified_keys.append(path)
                skipped_by_meta += 1
            continue

        # Both items with same title — need size/mimetype/content check
        common_items.append(path)
    meta_elapsed = time.time() - t0

    print(f"  {meta_elapsed:.1f}s ({len(common_keys) / max(meta_elapsed, 0.001):,.0f} entries/s)")
    print(f"  Modified (redirect/title): {skipped_by_meta:,}")
    print(f"  Items needing comparison: {len(common_items):,}")

    # Phase 3b: Parse ZIM binary for MIME types (no decompression needed).
    # This is ~1M entries/s vs ~300/s with get_item() on large files.
    peak_buf = 0
    content_compared = 0
    mime_mismatches = 0
    needs_content = []

    if common_items:
        print(f"\n  Parsing ZIM binary for MIME types...")
        t0 = time.time()
        _, _, old_entry_meta = _parse_zim_entries(args.old)
        _, _, new_entry_meta = _parse_zim_entries(args.new)
        print(f"  Parsed in {time.time() - t0:.1f}s, RSS: {_rss_mb()}")

        # Compare MIME types in memory (instant)
        for path in common_items:
            old_eid = old_index[path][3]
            new_eid = new_index[path][3]
            old_meta = old_entry_meta.get(old_eid)
            new_meta = new_entry_meta.get(new_eid)
            if old_meta is None or new_meta is None:
                needs_content.append(path)
                continue
            if old_meta[0] != new_meta[0]:  # MIME type differs
                modified_keys.append(path)
                mime_mismatches += 1
            else:
                needs_content.append(path)

        print(f"  MIME mismatches: {mime_mismatches:,}")
        print(f"  Need content comparison: {len(needs_content):,}")

    # Phase 3c: Content comparison via direct cluster decompression.
    # Bypasses libzim for ~140x faster cluster access by reading ZIM binary
    # directly and decompressing with zstandard.
    if needs_content:
        # Build items_to_hash dicts: {path: (entry_id, cluster_num, blob_num)}
        old_items = {}
        new_items = {}
        for path in needs_content:
            old_eid = old_index[path][3]
            new_eid = new_index[path][3]
            old_m = old_entry_meta.get(old_eid)
            new_m = new_entry_meta.get(new_eid)
            if old_m and new_m:
                old_items[path] = (old_eid, old_m[1], old_m[2])
                new_items[path] = (new_eid, new_m[1], new_m[2])

        del old_entry_meta, new_entry_meta  # free memory

        print(f"\n  Hashing {len(old_items):,} items via direct cluster decompression...")
        old_hashes = _hash_items_direct(args.old, old_items, "old")
        new_hashes = _hash_items_direct(args.new, new_items, "new")

        for path in needs_content:
            old_h = old_hashes.get(path)
            new_h = new_hashes.get(path)
            if old_h is None or new_h is None or old_h != new_h:
                modified_keys.append(path)

        del old_hashes, new_hashes

    changed_keys = added_keys + modified_keys

    print(f"\n  Total modified: {len(modified_keys):,}, Unchanged: {len(common_keys) - len(modified_keys):,}")
    print(f"  MIME mismatches: {mime_mismatches:,}, Content compared: {_fmt(content_compared)}")
    print(f"  Peak buffer: {_fmt(peak_buf)}, RSS: {_rss_mb()}")

    # Filter out skipped MIME types
    if args.skip_mime:
        skip_set = set(args.skip_mime)
        _, _, new_entry_meta_filt = _parse_zim_entries(args.new)
        before = len(changed_keys)
        filtered = []
        for path in changed_keys:
            info = new_index[path]
            if info[0]:  # redirect — always include
                filtered.append(path)
                continue
            meta = new_entry_meta_filt.get(info[3])
            if meta and meta[0] in skip_set:
                continue
            filtered.append(path)
        changed_keys = filtered
        del new_entry_meta_filt
        print(f"\n  Skipped {before - len(changed_keys):,} entries by MIME filter ({', '.join(skip_set)})")

    # Collect new-file metadata
    new_metadata = {}
    for key in new.metadata_keys:
        try:
            new_metadata[key] = new.get_metadata(key)
        except Exception:
            pass

    lang = "eng"
    if "Language" in new_metadata:
        try:
            lang = new_metadata["Language"].decode("utf-8")
        except Exception:
            pass

    main_entry = _resolve_main_entry(new)

    # Phase 4: Write overlay using direct cluster reads for items
    # Re-parse binary metadata for writing (we freed it earlier to save memory)
    _, _, new_entry_meta_w = _parse_zim_entries(args.new)

    # Separate redirects from items, group items by cluster for fast reading
    redirect_entries = []  # (path, title, target)
    item_entries = {}      # cluster_num -> [(path, title, mime, blob_num)]
    for path in changed_keys:
        info = new_index[path]
        is_redirect, title, target_or_none, eid = info
        if is_redirect:
            redirect_entries.append((path, title, target_or_none))
        else:
            meta = new_entry_meta_w.get(eid)
            if meta:
                mime_str, cnum, bnum = meta
                item_entries.setdefault(cnum, []).append((path, title, mime_str, bnum))
            else:
                # Fallback: use libzim (rare edge case)
                item_entries.setdefault(-1, []).append((path, title, None, eid))

    del new_entry_meta_w
    total_items = sum(len(v) for v in item_entries.values())

    print(f"\nWriting overlay: {args.output}")
    print(f"  {len(redirect_entries):,} redirects, {total_items:,} items across {len(item_entries):,} clusters")
    content_bytes = 0
    with Creator(args.output).config_indexing(False, lang) as creator:
        if main_entry:
            creator.set_mainpath(main_entry)

        creator.add_metadata(VERSION_META_KEY, str(OVERLAY_VERSION))
        creator.add_metadata(BASE_UUID_META_KEY, str(old.uuid))
        creator.add_metadata(BASE_CHECKSUM_META_KEY, old.checksum)
        creator.add_metadata(TARGET_UUID_META_KEY, str(new.uuid))
        creator.add_metadata(PARENT_UUID_META_KEY, str(old.uuid))
        creator.add_metadata(DELETION_META_KEY, json.dumps(sorted(removed_keys)))

        for key in ("Title", "Description", "Creator", "Publisher", "Date",
                    "Language", "Tags", "Name", "Source", "Flavour", "Scraper"):
            if key in new_metadata:
                val = new_metadata[key]
                creator.add_metadata(key, val if isinstance(val, bytes) else val)

        if "Illustration_48x48@1" in new_metadata:
            creator.add_metadata("Illustration_48x48@1", new_metadata["Illustration_48x48@1"])

        # Write all redirects first (instant, no I/O needed)
        for path, title, target in tqdm(redirect_entries, desc="Write redirects", unit=" entries",
                bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]"):
            creator.add_redirection(path, title, target, {Hint.FRONT_ARTICLE: False})

        # Write items using direct cluster reading (cluster-sequential)
        with open(args.new, 'rb') as f:
            mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
            cluster_count_w = struct.unpack_from('<I', mm, 28)[0]
            cluster_ptr_pos_w = struct.unpack_from('<Q', mm, 48)[0]
            checksum_pos_w = struct.unpack_from('<Q', mm, 72)[0]
            cluster_ptrs_w = list(struct.unpack_from(f'<{cluster_count_w}Q', mm, cluster_ptr_pos_w))
            cluster_ptrs_w.append(checksum_pos_w)

            dctx = zstandard.ZstdDecompressor()

            with tqdm(total=total_items, desc="Write items", unit=" entries",
                      bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]") as pbar:
                for cnum in sorted(item_entries.keys()):
                    items = item_entries[cnum]

                    if cnum == -1:
                        # Fallback items (no binary metadata)
                        for path, title, _, eid in items:
                            item = new._get_entry_by_id(eid).get_item()
                            content = bytes(item.content)
                            content_bytes += len(content)
                            creator.add_item(ZimItem(path, title, item.mimetype, content))
                            pbar.update(1)
                        continue

                    start = cluster_ptrs_w[cnum]
                    end = cluster_ptrs_w[cnum + 1]
                    comp_size = end - start
                    comp_type = mm[start]
                    compressed = bytes(mm[start + 1:end])

                    if comp_size > 100 * 1024 * 1024:
                        # Large cluster: stream decompress for single-blob items
                        if comp_type in (4, 5) and len(items) == 1:
                            path, title, mime_str, bnum = items[0]
                            chunks = []
                            reader = dctx.stream_reader(compressed)
                            while True:
                                chunk = reader.read(4 * 1024 * 1024)
                                if not chunk:
                                    break
                                chunks.append(chunk)
                            content = b''.join(chunks)
                            content_bytes += len(content)
                            creator.add_item(ZimItem(path, title, mime_str, content))
                            pbar.update(1)
                        else:
                            # Multi-blob large cluster — fallback to libzim
                            for path, title, mime_str, bnum in items:
                                try:
                                    item = new.get_entry_by_path(path).get_item()
                                    content = bytes(item.content)
                                    content_bytes += len(content)
                                    creator.add_item(ZimItem(path, title, item.mimetype, content))
                                except Exception:
                                    pass
                                pbar.update(1)
                        continue

                    # Normal cluster: decompress and extract blobs
                    if comp_type in (4, 5):
                        try:
                            decompressed = dctx.decompress(compressed,
                                max_output_size=max(len(compressed) * 30, 64 * 1024 * 1024))
                        except zstandard.ZstdError:
                            chunks = []
                            reader = dctx.stream_reader(compressed)
                            while True:
                                chunk = reader.read(1024 * 1024)
                                if not chunk:
                                    break
                                chunks.append(chunk)
                            decompressed = b''.join(chunks)
                    elif comp_type == 1:
                        decompressed = compressed
                    else:
                        for path, title, mime_str, bnum in items:
                            pbar.update(1)
                        continue

                    first_offset = struct.unpack_from('<I', decompressed, 0)[0]
                    n_blobs = first_offset // 4
                    if n_blobs > 0:
                        offsets = struct.unpack_from(f'<{n_blobs}I', decompressed, 0)
                    else:
                        for path, title, mime_str, bnum in items:
                            pbar.update(1)
                        continue

                    for path, title, mime_str, bnum in items:
                        if bnum >= n_blobs:
                            pbar.update(1)
                            continue
                        blob_start = offsets[bnum]
                        blob_end = offsets[bnum + 1] if bnum + 1 < n_blobs else len(decompressed)
                        content = decompressed[blob_start:blob_end]
                        content_bytes += len(content)
                        creator.add_item(ZimItem(path, title, mime_str, content))
                        pbar.update(1)

            mm.close()

    overlay_size = os.path.getsize(args.output)
    print(f"\nDone. Overlay: {_fmt(overlay_size)} "
          f"({len(changed_keys):,} entries, {_fmt(content_bytes)} content)")
    print(f"vs full download: {_fmt(new.filesize)} "
          f"({overlay_size * 100 / new.filesize:.1f}% of full)")
    print(f"Final RSS: {_rss_mb()}")


def cmd_apply(args):
    """Flatten base + overlays into a single updated ZIM."""
    reader = OverlayReader(args.base, args.overlays)

    base_size = reader.base.filesize
    overlay_sizes = sum(ov.filesize for ov in reader.overlays)
    print(f"Base: {_fmt(base_size)}, Overlays: {_fmt(overlay_sizes)} ({len(reader.overlays)} layers)")
    print(f"Initial RSS: {_rss_mb()}")

    top = reader.overlays[-1] if reader.overlays else reader.base

    lang = "eng"
    try:
        lang = top.get_metadata("Language").decode("utf-8")
    except Exception:
        pass

    main_entry = _resolve_main_entry(top)

    # Pre-collect paths so we can show a progress bar
    print("Collecting entry paths...")
    all_paths = list(reader.iter_paths())
    print(f"  {len(all_paths):,} live entries")

    print(f"Writing {args.output}...")
    with Creator(args.output).config_indexing(True, lang) as creator:
        if main_entry:
            creator.set_mainpath(main_entry)

        for key in top.metadata_keys:
            if key.startswith("zimdiff.") or key == "Counter":
                continue
            try:
                val = top.get_metadata(key)
                creator.add_metadata(key, val)
            except Exception:
                pass

        content_bytes = 0
        with tqdm(all_paths, desc="Writing", unit=" entries",
                  bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]") as pbar:
            for path in pbar:
                result = reader.get_entry(path)
                if result is None:
                    continue
                _, entry = result
                if entry.is_redirect:
                    target = entry.get_redirect_entry().path
                    creator.add_redirection(path, entry.title, target,
                                            {Hint.FRONT_ARTICLE: False})
                else:
                    item = entry.get_item()
                    content = bytes(item.content)
                    content_bytes += len(content)
                    creator.add_item(ZimItem(path, entry.title, item.mimetype, content))

    output_size = os.path.getsize(args.output)
    print(f"Done. {len(all_paths):,} entries, {_fmt(output_size)}")
    print(f"Final RSS: {_rss_mb()}")


def cmd_info(args):
    """Show information about an overlay ZIM."""
    archive = Archive(args.overlay)
    meta = _read_overlay_metadata(archive)

    if meta is None:
        print(f"{args.overlay} is not a zimdiff overlay (no zimdiff metadata).")
        print(f"  Entries: {archive.entry_count:,}")
        print(f"  Size: {_fmt(archive.filesize)}")
        return

    print(f"ZIM Overlay v{meta['version']}")
    print(f"File:        {args.overlay}")
    print(f"Size:        {_fmt(archive.filesize)}")
    print(f"Base UUID:   {meta['base_uuid']}")
    print(f"Target UUID: {meta['target_uuid']}")
    print(f"Parent UUID: {meta['parent_uuid']}")
    print()

    entries = list(_iter_entries(archive))
    items = [(p, e) for p, e in entries if not e.is_redirect]
    redirects = [(p, e) for p, e in entries if e.is_redirect]

    print(f"Changed entries: {len(entries):,} ({len(items):,} items, {len(redirects):,} redirects)")
    print(f"Deleted paths:   {len(meta['deleted']):,}")
    print()

    total_content = sum(e.get_item().size for _, e in items)
    print(f"Content payload: {_fmt(total_content)} (uncompressed)")

    for key in ("Title", "Date", "Description", "Creator"):
        try:
            val = archive.get_metadata(key).decode("utf-8")
            print(f"{key}: {val}")
        except Exception:
            pass

    if items:
        print()
        print("Largest entries:")
        by_size = sorted(items, key=lambda x: x[1].get_item().size, reverse=True)
        for path, entry in by_size[:10]:
            print(f"  {entry.get_item().size:>12,} bytes  {path[:70]}")

    if meta["deleted"]:
        print()
        n = min(10, len(meta["deleted"]))
        print(f"Sample deleted paths ({n} of {len(meta['deleted']):,}):")
        for path in meta["deleted"][:n]:
            print(f"  {path}")


def cmd_verify(args):
    """Verify that base + overlays matches a reference ZIM."""
    reader = OverlayReader(args.base, args.overlays)
    ref = Archive(args.reference)

    print(f"Reference: {ref.entry_count:,} entries ({_fmt(ref.filesize)})")
    print(f"Base + {len(reader.overlays)} overlay(s)")
    print(f"Initial RSS: {_rss_mb()}")

    # Index reference
    ref_index = _index_archive(ref, "reference")
    ref_paths = set(ref_index.keys())

    # Collect overlay view paths
    print("Collecting overlay view paths...")
    overlay_paths = set(reader.iter_paths())

    missing = ref_paths - overlay_paths
    extra = overlay_paths - ref_paths

    errors = 0
    if missing:
        print(f"\nMISSING from overlay view: {len(missing):,}")
        for p in sorted(missing)[:10]:
            print(f"  {p}")
        errors += len(missing)

    if extra:
        print(f"\nEXTRA in overlay view: {len(extra):,}")
        for p in sorted(extra)[:10]:
            print(f"  {p}")
        errors += len(extra)

    common = sorted(overlay_paths & ref_paths)
    mismatches = 0

    with tqdm(common, desc="Verifying content", unit=" entries",
              bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]") as pbar:
        for path in pbar:
            ref_entry = ref.get_entry_by_path(path)
            result = reader.get_entry(path)
            if result is None:
                mismatches += 1
                continue
            _, ov_entry = result

            if ref_entry.is_redirect != ov_entry.is_redirect:
                mismatches += 1
                continue

            if ref_entry.is_redirect:
                if ref_entry.get_redirect_entry().path != ov_entry.get_redirect_entry().path:
                    mismatches += 1
                continue

            if bytes(ref_entry.get_item().content) != bytes(ov_entry.get_item().content):
                mismatches += 1

    errors += mismatches
    print(f"\nEntries checked: {len(common):,}")
    print(f"Content mismatches: {mismatches:,}")
    print(f"Total errors: {errors:,}")
    print(f"Final RSS: {_rss_mb()}")

    if errors == 0:
        print("\nVERIFIED: overlay view matches reference exactly.")
    else:
        print("\nFAILED: differences found.")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Overlay-based incremental updates for ZIM files"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_diff = sub.add_parser("diff",
        help="Generate overlay ZIM from old and new ZIM files (server-side)")
    p_diff.add_argument("old", help="Old (base) ZIM file")
    p_diff.add_argument("new", help="New (target) ZIM file")
    p_diff.add_argument("-o", "--output", required=True, help="Output overlay ZIM")
    p_diff.add_argument("--skip-mime", action="append", default=[],
        help="Skip entries with these MIME types (can be repeated)")

    p_apply = sub.add_parser("apply",
        help="Flatten base + overlays into a single ZIM (client-side)")
    p_apply.add_argument("base", help="Base ZIM file")
    p_apply.add_argument("overlays", nargs="+", help="Overlay ZIM file(s), oldest first")
    p_apply.add_argument("-o", "--output", required=True, help="Output ZIM file")

    p_info = sub.add_parser("info", help="Show overlay information")
    p_info.add_argument("overlay", help="Overlay ZIM file")

    p_verify = sub.add_parser("verify",
        help="Verify base + overlays matches a reference ZIM")
    p_verify.add_argument("base", help="Base ZIM file")
    p_verify.add_argument("overlays", nargs="+", help="Overlay ZIM file(s)")
    p_verify.add_argument("--reference", required=True, help="Reference (target) ZIM file")

    args = parser.parse_args()
    {"diff": cmd_diff, "apply": cmd_apply, "info": cmd_info, "verify": cmd_verify}[args.command](args)


if __name__ == "__main__":
    main()
