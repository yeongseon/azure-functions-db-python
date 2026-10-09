#!/usr/bin/env bash
set -euo pipefail

case "${EVENT_NAME:-}" in
  pull_request)
    git diff --name-only --no-renames "${BASE_SHA:?}...${HEAD_SHA:?}"
    ;;
  push)
    before=${BEFORE_SHA:-}
    sha=${SHA:?}
    [[ -n "$before" && ! "$before" =~ ^0+$ ]]
    git cat-file -e "$before^{commit}" 2>/dev/null
    git merge-base --is-ancestor "$before" "$sha"
    git diff --name-only --no-renames "$before" "$sha"
    ;;
  *)
    exit 1
    ;;
esac
