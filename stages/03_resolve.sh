#!/usr/bin/env bash
set -euo pipefail

cwd=$(pwd)

src_root="/mnt/data/bblib/biobricks-ai"
dst_root="$cwd/docs"

process_brick() {
    brick_dir="$1"
    [ -d "$brick_dir" ] || return 0
    brick="$(basename "$brick_dir")"

    dst="$dst_root"
    mkdir -p "$dst"

    # Copy README.md (one level under brick_dir)
    find "$brick_dir" -mindepth 2 -name "*.md" -exec bash -c 'f="$1"; b="$2"; d="$3"; base=$(basename "$f"); cp "$f" "$d/${b}__${base}"' _ {} "$brick" "$dst" \;

    # # Recursively copy, resolving all symlinks
    # for brick_subdir in "$brick_dir"/*/brick; do
    #     [ -d "$brick_subdir" ] || continue
    #     cp -aLul "$brick_subdir"/. "$dst"/
    # done
}

export -f process_brick
export src_root
export dst_root

find "$src_root" -mindepth 1 -maxdepth 1 -type d | parallel --bar -j0 --will-cite process_brick {}