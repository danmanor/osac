from __future__ import annotations

import ast
import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
NETWORKING_TESTS = "tests/e2e/vmaas/regression/networking/**"
NETRIS_MARKER = "requires_netris_vmaas"


def _pytest_markers(path: Path) -> set[str]:
    module = ast.parse(path.read_text())
    for node in module.body:
        if not isinstance(node, ast.Assign):
            continue
        if not any(isinstance(target, ast.Name) and target.id == "pytestmark" for target in node.targets):
            continue
        marker_nodes = node.value.elts if isinstance(node.value, (ast.List, ast.Tuple)) else [node.value]
        markers = set()
        for marker in marker_nodes:
            if isinstance(marker, ast.Attribute):
                markers.add(marker.attr)
            if isinstance(marker, ast.Call) and isinstance(marker.func, ast.Attribute):
                markers.add(marker.func.attr)
        return markers
    return set()


def test_netris_vmaas_tests_have_a_dedicated_marker() -> None:
    test_file = ROOT / NETWORKING_TESTS.removesuffix("/**") / "test_vmaas_networking.py"
    assert NETRIS_MARKER in _pytest_markers(test_file)


def test_generic_vmaas_workflow_excludes_netris_tests() -> None:
    workflow = yaml.safe_load((ROOT / ".github/workflows/e2e-vmaas-full-install.yml").read_text())
    test_marker = workflow["jobs"]["e2e-vmaas-full-install"]["with"]["test-marker"]
    assert "and not requires_netris_vmaas" in test_marker


def test_generic_vmaas_path_filter_skips_netris_only_tests() -> None:
    filters = yaml.safe_load((ROOT / ".github/filters/ci-filters.yml").read_text())
    assert f"!{NETWORKING_TESTS}" in filters["vmaas-outside-known-safe"]


def test_bmaas_and_vmaas_netris_are_separate_manual_suites() -> None:
    policy = yaml.safe_load((ROOT / ".github/e2e-policy.yml").read_text())
    jobs = policy["jobs"]
    assert jobs["e2e-netris"]["manual_only"] is True
    assert jobs["e2e-netris"]["suite"] == "netris"
    assert jobs["e2e-vmaas-netris"]["manual_only"] is True
    assert jobs["e2e-vmaas-netris"]["suite"] == "vmaas-netris"


def test_vmaas_netris_calls_the_netris_runner_with_vmaas_profile() -> None:
    workflow = yaml.safe_load((ROOT / ".github/workflows/e2e-vmaas-netris-full-install.yml").read_text())
    job = workflow["jobs"]["e2e-vmaas-netris"]
    assert "/e2e-bmaas-netris-full-install.yml@" in job["uses"]
    assert job["with"]["suite"] == "vmaas"
    assert job["with"]["osac-profile"] == "cudn-evpn-netris-test"
    assert re.fullmatch(r"[0-9a-f]{40}", job["with"]["test-infra-sha"])
