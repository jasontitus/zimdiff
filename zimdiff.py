#!/usr/bin/env python3
"""zimdiff - Generate and apply content-level diffs for ZIM files.

Usage:
    zimdiff diff <old.zim> <new.zim> <output.zimdiff>
    zimdiff patch <old.zim> <patch.zimdiff> <output.zim>
    zimdiff info <patch.zimdiff>
"""

import argparse
import io
import json
import sys
import tarfile
import time

from libzim.reader import Archive
from libzim.writer import Creator, Item, StringProvider, Hint


DIFF_VERSION = 1


class ZimItem(Item):
    """An item for writing into a ZIM file."""

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
    """Yield (path, entry) for all accessible entries in an archive."""
    for i in range(archive.all_entry_count):
        entry = archive._get_entry_by_id(i)
        if archive.has_entry_by_path(entry.path):
            yield entry.path, entry


def _add_to_tar(tar, name, data):
    """Add a bytes blob to a tar archive."""
    ti = tarfile.TarInfo(name=name)
    ti.size = len(data)
    tar.addfile(ti, io.BytesIO(data))


def compute_diff(old_path, new_path, output_path):
    """Generate a diff between two ZIM files."""
    old = Archive(old_path)
    new = Archive(new_path)

    print(f"Old: {old.all_entry_count} entries ({old.filesize / 1024 / 1024:.1f} MB)")
    print(f"New: {new.all_entry_count} entries ({new.filesize / 1024 / 1024:.1f} MB)")

    # Build path indexes
    old_paths = {}
    for path, entry in _iter_entries(old):
        old_paths[path] = entry

    new_paths = {}
    for path, entry in _iter_entries(new):
        new_paths[path] = entry

    added_keys = set(new_paths) - set(old_paths)
    removed_keys = set(old_paths) - set(new_paths)
    common_keys = set(old_paths) & set(new_paths)

    # Collect changes
    entries = []  # list of entry descriptors
    blobs = {}  # index -> bytes

    entry_index = 0
    modified_count = 0

    t0 = time.time()

    # Added entries
    for path in sorted(added_keys):
        entry = new_paths[path]
        info = {"op": "add", "path": path, "title": entry.title, "idx": entry_index}
        if entry.is_redirect:
            info["redirect"] = entry.get_redirect_entry().path
        else:
            item = entry.get_item()
            content = bytes(item.content)
            info["mimetype"] = item.mimetype
            info["size"] = len(content)
            blobs[entry_index] = content
        entries.append(info)
        entry_index += 1

    # Check common entries for modifications
    for path in sorted(common_keys):
        old_entry = old_paths[path]
        new_entry = new_paths[path]
        changed = False

        # Title change check
        title_changed = old_entry.title != new_entry.title

        # Type change (redirect <-> item)
        if old_entry.is_redirect != new_entry.is_redirect:
            changed = True
        elif old_entry.is_redirect and new_entry.is_redirect:
            # Both redirects - check target
            if old_entry.get_redirect_entry().path != new_entry.get_redirect_entry().path:
                changed = True
            elif title_changed:
                changed = True
        else:
            # Both items - compare content
            old_content = bytes(old_entry.get_item().content)
            new_content = bytes(new_entry.get_item().content)
            if old_content != new_content or title_changed:
                changed = True

        if not changed:
            continue

        modified_count += 1
        info = {"op": "modify", "path": path, "title": new_entry.title, "idx": entry_index}

        if new_entry.is_redirect:
            info["redirect"] = new_entry.get_redirect_entry().path
        else:
            item = new_entry.get_item()
            content = bytes(item.content)
            info["mimetype"] = item.mimetype
            info["size"] = len(content)
            blobs[entry_index] = content

        entries.append(info)
        entry_index += 1

    elapsed = time.time() - t0

    # Collect metadata from new file
    metadata = {}
    for key in new.metadata_keys:
        try:
            val = new.get_metadata(key)
            # Store as hex for binary values, string for text
            try:
                metadata[key] = val.decode("utf-8")
            except UnicodeDecodeError:
                metadata[key] = {"hex": val.hex()}
        except Exception:
            pass

    # Resolve main entry to its final content path, since the main entry
    # is often a special redirect not visible via has_entry_by_path
    main_entry = None
    if new.has_main_entry:
        me = new.main_entry
        while me.is_redirect:
            me = me.get_redirect_entry()
        main_entry = me.path

    manifest = {
        "version": DIFF_VERSION,
        "old_uuid": str(old.uuid),
        "new_uuid": str(new.uuid),
        "old_checksum": old.checksum,
        "main_entry": main_entry,
        "metadata": metadata,
        "removed": sorted(removed_keys),
        "entries": entries,
    }

    # Write diff file
    print(f"Compared {len(common_keys)} common entries in {elapsed:.1f}s")
    print(f"  Added:    {len(added_keys)}")
    print(f"  Removed:  {len(removed_keys)}")
    print(f"  Modified: {modified_count}")
    print(f"  Blobs:    {len(blobs)} ({sum(len(b) for b in blobs.values()) / 1024 / 1024:.2f} MB uncompressed)")

    print(f"Writing {output_path}...")
    with tarfile.open(output_path, "w:gz") as tar:
        # Write content blobs first (streaming-friendly)
        for idx in sorted(blobs):
            _add_to_tar(tar, f"entries/{idx:07d}", blobs[idx])

        # Write manifest last
        manifest_bytes = json.dumps(manifest, indent=2, ensure_ascii=False).encode("utf-8")
        _add_to_tar(tar, "manifest.json", manifest_bytes)

    import os
    size = os.path.getsize(output_path)
    print(f"Done. Diff size: {size / 1024 / 1024:.2f} MB")


def apply_patch(old_path, diff_path, output_path):
    """Apply a diff to an old ZIM file to produce a new ZIM file."""
    old = Archive(old_path)

    print(f"Old ZIM: {old.all_entry_count} entries")
    print(f"Reading diff: {diff_path}")

    with tarfile.open(diff_path, "r:*") as tar:
        manifest_bytes = tar.extractfile("manifest.json").read()
        manifest = json.loads(manifest_bytes)

        if manifest["version"] != DIFF_VERSION:
            print(f"Error: unsupported diff version {manifest['version']}", file=sys.stderr)
            sys.exit(1)

        if manifest["old_uuid"] != str(old.uuid):
            print(
                f"Warning: UUID mismatch. Diff expects {manifest['old_uuid']}, "
                f"got {old.uuid}",
                file=sys.stderr,
            )

        removed = set(manifest["removed"])
        diff_entries = {}
        for info in manifest["entries"]:
            diff_entries[info["path"]] = info

        added_count = sum(1 for e in manifest["entries"] if e["op"] == "add")
        modified_count = sum(1 for e in manifest["entries"] if e["op"] == "modify")
        print(f"  Added: {added_count}, Modified: {modified_count}, Removed: {len(removed)}")

        # Pre-load blobs from tar
        blob_data = {}
        for info in manifest["entries"]:
            if "redirect" not in info and "size" in info:
                blob_name = f"entries/{info['idx']:07d}"
                blob_data[info["path"]] = tar.extractfile(blob_name).read()

        # Determine language for indexing
        lang = "eng"
        meta = manifest.get("metadata", {})
        if "Language" in meta and isinstance(meta["Language"], str):
            lang = meta["Language"]

        print(f"Writing {output_path}...")
        with Creator(output_path).config_indexing(True, lang) as creator:
            # Set main entry
            if manifest.get("main_entry"):
                creator.set_mainpath(manifest["main_entry"])

            # Add metadata from diff
            for key, value in meta.items():
                if key == "Counter":
                    continue  # auto-generated
                if isinstance(value, dict) and "hex" in value:
                    creator.add_metadata(key, bytes.fromhex(value["hex"]))
                else:
                    creator.add_metadata(key, value)

            # Copy unchanged entries from old ZIM
            copied = 0
            for path, entry in _iter_entries(old):
                if path in removed:
                    continue
                if path in diff_entries:
                    continue

                if entry.is_redirect:
                    target = entry.get_redirect_entry().path
                    # Skip if target was removed
                    if target in removed and target not in diff_entries:
                        continue
                    creator.add_redirection(
                        path, entry.title, target, {Hint.FRONT_ARTICLE: False}
                    )
                else:
                    item = entry.get_item()
                    content = bytes(item.content)
                    creator.add_item(ZimItem(path, entry.title, item.mimetype, content))
                copied += 1

            # Add new/modified entries from diff
            for info in manifest["entries"]:
                path = info["path"]
                title = info.get("title", "")

                if "redirect" in info:
                    creator.add_redirection(
                        path, title, info["redirect"], {Hint.FRONT_ARTICLE: False}
                    )
                else:
                    content = blob_data[path]
                    mimetype = info["mimetype"]
                    creator.add_item(ZimItem(path, title, mimetype, content))

            print(f"  Copied {copied} unchanged, applied {len(manifest['entries'])} changes")

    print("Done.")


def show_info(diff_path):
    """Display information about a diff file."""
    import os

    file_size = os.path.getsize(diff_path)

    with tarfile.open(diff_path, "r:*") as tar:
        manifest_bytes = tar.extractfile("manifest.json").read()
        manifest = json.loads(manifest_bytes)

    added = [e for e in manifest["entries"] if e["op"] == "add"]
    modified = [e for e in manifest["entries"] if e["op"] == "modify"]
    removed = manifest.get("removed", [])

    added_items = [e for e in added if "redirect" not in e]
    added_redirects = [e for e in added if "redirect" in e]
    modified_items = [e for e in modified if "redirect" not in e]
    modified_redirects = [e for e in modified if "redirect" in e]

    added_size = sum(e.get("size", 0) for e in added)
    modified_size = sum(e.get("size", 0) for e in modified)

    print(f"ZIM Diff v{manifest['version']}")
    print(f"File size:  {file_size / 1024 / 1024:.2f} MB")
    print(f"Old UUID:   {manifest['old_uuid']}")
    print(f"New UUID:   {manifest['new_uuid']}")
    print()
    print(f"Added:      {len(added):>5}  ({len(added_items)} items, {len(added_redirects)} redirects)")
    print(f"Modified:   {len(modified):>5}  ({len(modified_items)} items, {len(modified_redirects)} redirects)")
    print(f"Removed:    {len(removed):>5}")
    print()
    print(f"Added content:    {added_size / 1024 / 1024:.2f} MB (uncompressed)")
    print(f"Modified content: {modified_size / 1024 / 1024:.2f} MB (uncompressed)")
    print(f"Total payload:    {(added_size + modified_size) / 1024 / 1024:.2f} MB (uncompressed)")

    # Show metadata
    meta = manifest.get("metadata", {})
    if meta:
        print()
        print("New file metadata:")
        for key in sorted(meta):
            val = meta[key]
            if isinstance(val, dict):
                print(f"  {key}: ({len(val.get('hex', '')) // 2} bytes binary)")
            elif len(str(val)) > 80:
                print(f"  {key}: {str(val)[:80]}...")
            else:
                print(f"  {key}: {val}")

    # Show top modified entries by size
    if modified_items:
        print()
        print("Largest modified entries:")
        by_size = sorted(modified_items, key=lambda e: e.get("size", 0), reverse=True)
        for e in by_size[:10]:
            print(f"  {e.get('size', 0):>10,} bytes  {e['path'][:70]}")


def main():
    parser = argparse.ArgumentParser(
        description="Generate and apply content-level diffs for ZIM files"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_diff = sub.add_parser("diff", help="Generate diff between two ZIM files")
    p_diff.add_argument("old", help="Old ZIM file")
    p_diff.add_argument("new", help="New ZIM file")
    p_diff.add_argument("output", help="Output diff file (.zimdiff)")

    p_patch = sub.add_parser("patch", help="Apply diff to produce a new ZIM file")
    p_patch.add_argument("old", help="Old ZIM file")
    p_patch.add_argument("diff", help="Diff file (.zimdiff)")
    p_patch.add_argument("output", help="Output ZIM file")

    p_info = sub.add_parser("info", help="Show information about a diff file")
    p_info.add_argument("diff", help="Diff file (.zimdiff)")

    args = parser.parse_args()

    if args.command == "diff":
        compute_diff(args.old, args.new, args.output)
    elif args.command == "patch":
        apply_patch(args.old, args.diff, args.output)
    elif args.command == "info":
        show_info(args.diff)


if __name__ == "__main__":
    main()
