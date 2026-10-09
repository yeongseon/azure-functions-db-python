#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
output=${1:-${GITHUB_OUTPUT:?}}
paths=$(mktemp)
result=$(mktemp)
trap 'cat "$result" >> "$output"; rm -f "$paths" "$result"' EXIT

fail_safe() {
  printf 'docs_only=false\ndocs_changed=true\nfull_required=true\n' > "$result"
}

fail_safe

if [[ "${EVENT_NAME:-}" == pull_request && "${SAME_REPOSITORY:-}" != true ]]; then
  git fetch --no-tags origin "refs/pull/${PR_NUMBER:?}/head" || exit 0
fi

"$script_dir/ci_changed_paths.sh" > "$paths" || exit 0
"$script_dir/ci_classify_changes.sh" < "$paths" > "$result" || exit 0
