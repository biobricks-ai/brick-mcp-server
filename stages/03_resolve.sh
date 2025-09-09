#!/usr/bin/env bash
set -euo pipefail

cwd=$(pwd)

src_root="/mnt/data/bblib/biobricks-ai"
dst_root="$cwd/mcp"

process_brick() {
    brick_dir="$1"
    [ -d "$brick_dir" ] || return 0
    brick="$(basename "$brick_dir")"

    dst="$dst_root/$brick"
    mkdir -p "$dst"

    # Copy README.md (one level under brick_dir)
    find "$brick_dir" -mindepth 2 -maxdepth 2 -name "README.md" -exec cp {} "$dst"/ \;

    # Recursively copy, resolving all symlinks
    for brick_subdir in "$brick_dir"/*/brick; do
        [ -d "$brick_subdir" ] || continue
        cp -aL "$brick_subdir"/. "$dst"/
    done
}

export -f process_brick
export src_root
export dst_root

find "$src_root" -mindepth 1 -maxdepth 1 -type d | parallel --bar --will-cite process_brick {}