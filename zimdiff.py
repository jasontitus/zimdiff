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
import sys
import time

from libzim.reader import Archive
from libzim.writer import Creator, Item, StringProvider, Hint


OVERLAY_VERSION = 2
DELETION_META_KEY = "zimdiff.deleted"
BASE_UUID_META_KEY = "zimdiff.base_uuid"
BASE_CHECKSUM_META_KEY = "zimdiff.base_checksum"
TARGET_UUID_META_KEY = "zimdiff.target_uuid"
VERSION_META_KEY = "zimdiff.version"
# Stacking: the overlay also records what it was built against so
# sequential overlays can be validated
PARENT_UUID_META_KEY = "zimdiff.parent_uuid"


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
# Overlay reader: presents base + overlays as a unified view
# ---------------------------------------------------------------------------


class OverlayReader:
    """Read from a base ZIM with overlay ZIMs stacked on top.

    Entry resolution order: last overlay → ... → first overlay → base.
    If an entry is in any overlay's deletion list and no later overlay
    re-adds it, it is considered deleted.
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
        # Check overlays newest-first
        for i in range(len(self.overlays) - 1, -1, -1):
            overlay = self.overlays[i]
            if overlay.has_entry_by_path(path):
                return overlay, overlay.get_entry_by_path(path)
            if path in self._deleted_sets[i]:
                return None
        # Fall back to base
        if self.base.has_entry_by_path(path):
            return self.base, self.base.get_entry_by_path(path)
        return None

    def iter_paths(self):
        """Yield all live paths across base + overlays."""
        # Collect all paths, tracking which are deleted
        all_deleted = set()
        overlay_paths = set()

        # Walk overlays newest-first to build deletion set
        for i in range(len(self.overlays) - 1, -1, -1):
            for path, _ in _iter_entries(self.overlays[i]):
                overlay_paths.add(path)
            # Paths in this deletion list are dead unless a newer overlay added them
            for path in self._deleted_sets[i]:
                if path not in overlay_paths:
                    all_deleted.add(path)

        # Yield overlay paths
        yield from overlay_paths

        # Yield base paths that aren't deleted or overridden
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

    print(f"Old: {old.entry_count} entries ({old.filesize / 1024 / 1024:.1f} MB) [{old.uuid}]")
    print(f"New: {new.entry_count} entries ({new.filesize / 1024 / 1024:.1f} MB) [{new.uuid}]")

    old_paths = {}
    for path, entry in _iter_entries(old):
        old_paths[path] = entry

    new_paths = {}
    for path, entry in _iter_entries(new):
        new_paths[path] = entry

    added_keys = set(new_paths) - set(old_paths)
    removed_keys = set(old_paths) - set(new_paths)
    common_keys = set(old_paths) & set(new_paths)

    # Find modified entries
    modified_keys = set()
    t0 = time.time()
    for path in common_keys:
        old_e = old_paths[path]
        new_e = new_paths[path]

        if old_e.title != new_e.title:
            modified_keys.add(path)
            continue
        if old_e.is_redirect != new_e.is_redirect:
            modified_keys.add(path)
            continue
        if old_e.is_redirect:
            if old_e.get_redirect_entry().path != new_e.get_redirect_entry().path:
                modified_keys.add(path)
            continue
        if bytes(old_e.get_item().content) != bytes(new_e.get_item().content):
            modified_keys.add(path)

    elapsed = time.time() - t0
    changed_keys = added_keys | modified_keys

    print(f"Compared {len(common_keys)} entries in {elapsed:.1f}s")
    print(f"  Added:     {len(added_keys)}")
    print(f"  Modified:  {len(modified_keys)}")
    print(f"  Removed:   {len(removed_keys)}")
    print(f"  Unchanged: {len(common_keys) - len(modified_keys)}")

    # Collect new-file metadata to propagate
    new_metadata = {}
    for key in new.metadata_keys:
        try:
            new_metadata[key] = new.get_metadata(key)
        except Exception:
            pass

    # Determine language
    lang = "eng"
    if "Language" in new_metadata:
        try:
            lang = new_metadata["Language"].decode("utf-8")
        except Exception:
            pass

    main_entry = _resolve_main_entry(new)

    print(f"Writing overlay: {args.output}")
    with Creator(args.output).config_indexing(False, lang) as creator:
        if main_entry:
            creator.set_mainpath(main_entry)

        # Write overlay metadata
        creator.add_metadata(VERSION_META_KEY, str(OVERLAY_VERSION))
        creator.add_metadata(BASE_UUID_META_KEY, str(old.uuid))
        creator.add_metadata(BASE_CHECKSUM_META_KEY, old.checksum)
        creator.add_metadata(TARGET_UUID_META_KEY, str(new.uuid))
        # For first overlay, parent == base
        creator.add_metadata(PARENT_UUID_META_KEY, str(old.uuid))
        creator.add_metadata(DELETION_META_KEY, json.dumps(sorted(removed_keys)))

        # Propagate essential content metadata from the new file
        for key in ("Title", "Description", "Creator", "Publisher", "Date",
                    "Language", "Tags", "Name", "Source", "Flavour", "Scraper"):
            if key in new_metadata:
                val = new_metadata[key]
                creator.add_metadata(key, val if isinstance(val, bytes) else val)

        if "Illustration_48x48@1" in new_metadata:
            creator.add_metadata("Illustration_48x48@1", new_metadata["Illustration_48x48@1"])

        # Write all changed/added entries
        content_bytes = 0
        for path in sorted(changed_keys):
            entry = new_paths[path]
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
    print(f"Done. Overlay: {overlay_size / 1024 / 1024:.1f} MB "
          f"({len(changed_keys)} entries, "
          f"{content_bytes / 1024 / 1024:.1f} MB content)")
    print(f"vs full download: {new.filesize / 1024 / 1024:.1f} MB "
          f"({overlay_size * 100 / new.filesize:.0f}% of full)")


def cmd_apply(args):
    """Flatten base + overlays into a single updated ZIM."""
    reader = OverlayReader(args.base, args.overlays)

    base_size = reader.base.filesize / 1024 / 1024
    overlay_sizes = sum(ov.filesize for ov in reader.overlays) / 1024 / 1024
    print(f"Base: {base_size:.1f} MB, Overlays: {overlay_sizes:.1f} MB ({len(reader.overlays)} layers)")

    # Use metadata from the newest overlay
    top = reader.overlays[-1] if reader.overlays else reader.base

    lang = "eng"
    try:
        lang = top.get_metadata("Language").decode("utf-8")
    except Exception:
        pass

    main_entry = _resolve_main_entry(top)

    print(f"Writing {args.output}...")
    with Creator(args.output).config_indexing(True, lang) as creator:
        if main_entry:
            creator.set_mainpath(main_entry)

        # Copy metadata from newest overlay (which has the target version's metadata)
        for key in top.metadata_keys:
            if key.startswith("zimdiff.") or key == "Counter":
                continue
            try:
                val = top.get_metadata(key)
                creator.add_metadata(key, val)
            except Exception:
                pass

        # Write all live entries
        count = 0
        for path in reader.iter_paths():
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
                creator.add_item(ZimItem(path, entry.title, item.mimetype, content))
            count += 1

    output_size = os.path.getsize(args.output) / 1024 / 1024
    print(f"Done. {count} entries, {output_size:.1f} MB")


def cmd_info(args):
    """Show information about an overlay ZIM."""
    archive = Archive(args.overlay)
    meta = _read_overlay_metadata(archive)

    if meta is None:
        print(f"{args.overlay} is not a zimdiff overlay (no zimdiff metadata).")
        print(f"  Entries: {archive.entry_count}")
        print(f"  Size: {archive.filesize / 1024 / 1024:.1f} MB")
        return

    print(f"ZIM Overlay v{meta['version']}")
    print(f"File:        {args.overlay}")
    print(f"Size:        {archive.filesize / 1024 / 1024:.1f} MB")
    print(f"Base UUID:   {meta['base_uuid']}")
    print(f"Target UUID: {meta['target_uuid']}")
    print(f"Parent UUID: {meta['parent_uuid']}")
    print()

    # Count overlay entries (excluding metadata-only)
    entries = list(_iter_entries(archive))
    items = [(p, e) for p, e in entries if not e.is_redirect]
    redirects = [(p, e) for p, e in entries if e.is_redirect]

    print(f"Changed entries: {len(entries)} ({len(items)} items, {len(redirects)} redirects)")
    print(f"Deleted paths:   {len(meta['deleted'])}")
    print()

    # Content size
    total_content = sum(e.get_item().size for _, e in items)
    print(f"Content payload: {total_content / 1024 / 1024:.1f} MB (uncompressed)")

    # Show content metadata
    for key in ("Title", "Date", "Description", "Creator"):
        try:
            val = archive.get_metadata(key).decode("utf-8")
            print(f"{key}: {val}")
        except Exception:
            pass

    # Largest entries
    if items:
        print()
        print("Largest entries:")
        by_size = sorted(items, key=lambda x: x[1].get_item().size, reverse=True)
        for path, entry in by_size[:10]:
            print(f"  {entry.get_item().size:>10,} bytes  {path[:70]}")

    if meta["deleted"]:
        print()
        print(f"Sample deleted paths ({min(10, len(meta['deleted']))} of {len(meta['deleted'])}):")
        for path in meta["deleted"][:10]:
            print(f"  {path}")


def cmd_verify(args):
    """Verify that base + overlays matches a reference ZIM."""
    reader = OverlayReader(args.base, args.overlays)
    ref = Archive(args.reference)

    print(f"Reference: {ref.entry_count} entries ({ref.filesize / 1024 / 1024:.1f} MB)")
    print(f"Base + {len(reader.overlays)} overlay(s)")

    # Build reference map
    ref_paths = {}
    for path, entry in _iter_entries(ref):
        ref_paths[path] = entry

    # Build overlay view
    overlay_paths = set(reader.iter_paths())

    missing = set(ref_paths) - overlay_paths
    extra = overlay_paths - set(ref_paths)

    errors = 0
    if missing:
        print(f"\nMISSING from overlay view: {len(missing)}")
        for p in sorted(missing)[:10]:
            print(f"  {p}")
        errors += len(missing)

    if extra:
        print(f"\nEXTRA in overlay view: {len(extra)}")
        for p in sorted(extra)[:10]:
            print(f"  {p}")
        errors += len(extra)

    # Compare content of common entries
    common = overlay_paths & set(ref_paths)
    mismatches = 0
    for path in sorted(common):
        ref_entry = ref_paths[path]
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
    print(f"\nEntries checked: {len(common)}")
    print(f"Content mismatches: {mismatches}")
    print(f"Total errors: {errors}")

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
