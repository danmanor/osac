#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "${script_dir}/../.." && pwd)"

uv run --project "${repo_root}" python "${script_dir}/test-networking-values.py"
python3 "${script_dir}/test-network-class-step.py"
python3 "${script_dir}/test-fabric-domain-values.py"
