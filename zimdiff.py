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
import json
import os
import resource
import sys
import time

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
    """Build a {path: entry_id} index for an archive, with progress bar.

    Returns dict mapping path -> entry_id (int), which is much lighter
    than holding entry objects.
    """
    total = archive.all_entry_count
    index = {}
    with tqdm(range(total), desc=f"Indexing {label}", unit=" entries",
              bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]") as pbar:
        for i in pbar:
            entry = archive._get_entry_by_id(i)
            if archive.has_entry_by_path(entry.path):
                index[entry.path] = i
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

    # Phase 1-2: Index both archives (store entry IDs, not objects)
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

    # Phase 3: Compare common entries
    # Optimization: check metadata (title, type, mimetype, size) before reading
    # full content. For large ZIMs most time is spent decompressing content from
    # disk, so skipping unnecessary reads is critical.
    modified_keys = []
    peak_buf = 0
    content_compared = 0
    skipped_by_meta = 0

    with tqdm(common_keys, desc="Comparing", unit=" entries", miniters=100,
              bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]{postfix}") as pbar:
        for path in pbar:
            old_e = old.get_entry_by_path(path)
            new_e = new.get_entry_by_path(path)

            # Fast metadata checks (no disk I/O for content)
            if old_e.title != new_e.title:
                modified_keys.append(path)
                skipped_by_meta += 1
                continue
            if old_e.is_redirect != new_e.is_redirect:
                modified_keys.append(path)
                skipped_by_meta += 1
                continue
            if old_e.is_redirect:
                if old_e.get_redirect_entry().path != new_e.get_redirect_entry().path:
                    modified_keys.append(path)
                    skipped_by_meta += 1
                continue

            old_item = old_e.get_item()
            new_item = new_e.get_item()

            # Size/mimetype differ → modified without reading content
            if old_item.size != new_item.size or old_item.mimetype != new_item.mimetype:
                modified_keys.append(path)
                skipped_by_meta += 1
                continue

            # Same size — must compare content bytes
            old_content = bytes(old_item.content)
            new_content = bytes(new_item.content)
            buf_size = len(old_content) + len(new_content)
            if buf_size > peak_buf:
                peak_buf = buf_size
            content_compared += buf_size

            if old_content != new_content:
                modified_keys.append(path)

            pbar.set_postfix_str(
                f"mod={len(modified_keys):,} skip={skipped_by_meta:,} read={_fmt(content_compared)}",
                refresh=False)

    changed_keys = added_keys + modified_keys

    print(f"  Modified: {len(modified_keys):,}, Unchanged: {len(common_keys) - len(modified_keys):,}")
    print(f"  Skipped by metadata: {skipped_by_meta:,} (no content read needed)")
    print(f"  Content compared: {_fmt(content_compared)}, Peak buffer: {_fmt(peak_buf)}")
    print(f"  RSS: {_rss_mb()}")

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

    # Phase 4: Write overlay
    print(f"\nWriting overlay: {args.output}")
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

        with tqdm(sorted(changed_keys), desc="Writing overlay", unit=" entries",
                  bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]") as pbar:
            for path in pbar:
                entry = new.get_entry_by_path(path)
                if entry.is_redirect:
                    target = entry.get_redirect_entry().path
                    creator.add_redirection(path, entry.title, target,
                                            {Hint.FRONT_ARTICLE: False})
                else:
                    item = entry.get_item()
                    content = bytes(item.content)
                    content_bytes += len(content)
                    creator.add_item(ZimItem(path, entry.title, item.mimetype, content))

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
