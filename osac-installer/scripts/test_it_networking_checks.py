"""Unit tests for the installer networking integration-check helpers."""

from __future__ import annotations

import json
import runpy
import subprocess
import unittest
from pathlib import Path


CHECKS = runpy.run_path(str(Path(__file__).with_name("it-networking-checks.py")))


class ManifestsToJsonTests(unittest.TestCase):
    def test_parses_multiple_json_documents_from_kubectl(self) -> None:
        expected = [
            {"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "registration"}},
            {"apiVersion": "batch/v1", "kind": "Job", "metadata": {"name": "network-class"}},
        ]
        manifest = """\
apiVersion: v1
kind: ConfigMap
metadata:
  name: registration
---
apiVersion: batch/v1
kind: Job
metadata:
  name: network-class
"""
        kubectl_output = "\n".join(json.dumps(doc, indent=2) for doc in expected) + "\n"
        script_globals = CHECKS["manifests_to_json"].__globals__
        original_run = script_globals["run"]

        def fake_run(args: list[str], stdin: str | None = None, check: bool = True) -> subprocess.CompletedProcess:
            self.assertEqual(args, ["kubectl", "create", "--dry-run=client", "-o", "json", "-f", "-"])
            self.assertEqual(stdin, manifest)
            return subprocess.CompletedProcess(args, 0, stdout=kubectl_output, stderr="")

        script_globals["run"] = fake_run
        try:
            self.assertEqual(CHECKS["manifests_to_json"](manifest), expected)
        finally:
            script_globals["run"] = original_run


if __name__ == "__main__":
    unittest.main()
