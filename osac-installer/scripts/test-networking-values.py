#!/usr/bin/env python3
"""Render tests for the osac chart's networking values (global.networking).

Covers the values schema, render-time validation, NetworkClass step input,
admission policy, and networking values passed to the subcharts.
"""

from __future__ import annotations

import json
import os
import pathlib
import subprocess
import tempfile
import unittest
from typing import Any

import yaml

INSTALLER = pathlib.Path(__file__).resolve().parents[1]
CHART = INSTALLER / "charts/osac"
DEFAULT_VALUES = CHART / "ci/default-values.yaml"
HELM = os.environ.get("HELM_BIN", "helm")

PLAN_PATH = "global.networking.networkClass.addressPlan"
DEFAULTS_PATH = "global.networking.networkClass.defaults"


def services(vmaas: bool, bmaas: bool, caas: bool) -> dict:
    return {
        "global": {
            "services": {
                "vmaas": {"enabled": vmaas},
                "bmaas": {"enabled": bmaas},
                "caas": {"enabled": caas},
                "maas": {"enabled": caas},
            }
        }
    }


def networking(**values) -> dict:
    return {"global": {"networking": values}}


def merge(*overrides: dict) -> dict:
    result: dict = {}
    for override in overrides:
        _merge(result, override)
    return result


def _merge(dst: dict, src: dict) -> None:
    for key, value in src.items():
        if isinstance(value, dict) and isinstance(dst.get(key), dict):
            _merge(dst[key], value)
        else:
            dst[key] = value


def helm(*overrides: dict, args: tuple[str, ...] = ()) -> subprocess.CompletedProcess:
    with tempfile.NamedTemporaryFile("w", suffix=".json") as extra:
        json.dump(merge(*overrides), extra)
        extra.flush()
        return subprocess.run(
            [
                HELM,
                "template",
                "osac",
                str(CHART),
                "--namespace",
                "osac",
                "--values",
                str(DEFAULT_VALUES),
                "--values",
                extra.name,
                *args,
            ],
            capture_output=True,
            text=True,
        )


def render(*overrides: dict, args: tuple[str, ...] = ()) -> list[dict[str, Any]]:
    result = helm(*overrides, args=args)
    if result.returncode != 0:
        raise AssertionError(f"render failed: {result.stderr}")
    return [doc for doc in yaml.safe_load_all(result.stdout) if doc]


def find(docs: list[dict], kind: str, name: str) -> dict:
    for doc in docs:
        if doc.get("kind") == kind and doc["metadata"]["name"] == name:
            return doc
    raise AssertionError(f"{kind}/{name} not rendered")


def container_env(doc: dict, container: str | None = None) -> dict[str, str]:
    containers = doc["spec"]["template"]["spec"]["containers"]
    c = next(c for c in containers if container is None or c["name"] == container)
    return {e["name"]: e.get("value") for e in c.get("env", [])}


def step_input(docs: list[dict]) -> dict:
    return json.loads(container_env(find(docs, "Job", "create-network-class"))["NETWORK_CLASS_STEP_INPUT"])


class RenderFailureCase(unittest.TestCase):
    def assert_fails(self, message: str, *overrides: dict, args: tuple[str, ...] = ()):
        result = helm(*overrides, args=args)
        self.assertNotEqual(result.returncode, 0, f"rendered, expected failure containing {message!r}")
        if message == "missing properties":
            individual_missing_properties = (
                "fabric is required" in result.stderr and "lbVip is required" in result.stderr
            )
            grouped_missing_properties = (
                "missing properties" in result.stderr
                and "fabric" in result.stderr
                and "lbVip" in result.stderr
            )
            self.assertTrue(individual_missing_properties or grouped_missing_properties, result.stderr)
        elif message.startswith("/"):
            # Helm 3 reports dotted paths; Helm 4 reports JSON pointers.
            dotted_path = message.lstrip("/").replace("/", ".")
            self.assertTrue(
                message in result.stderr or dotted_path in result.stderr,
                f"expected {message!r} or {dotted_path!r} in {result.stderr!r}",
            )
        else:
            self.assertIn(message, result.stderr)


class RemovedKeysTest(RenderFailureCase):
    def test_removed_keys_are_rejected(self):
        cases = {
            "global.networking.legacyBackend": networking(legacyBackend={"name": "example"}),
            "top-level networkClass": {"networkClass": {"enabled": True}},
            "global.expertOverrides": {"global": {"expertOverrides": {"aap": True}}},
            "operator.controllers.networkingProvisioning": {
                "operator": {"controllers": {"networkingProvisioning": False}}
            },
            "defaults.enableNatGateway": networking(networkClass={"defaults": {"enableNatGateway": True}}),
            "NETWORK_STEPS_COLLECTION": {
                "aap": {"instanceGroups": {"clusterFulfillment": {"config": {"NETWORK_STEPS_COLLECTION": "ci.steps"}}}}
            },
            "SERVER_SSH_KEY": {"aap": {"instanceGroups": {"clusterFulfillment": {"secret": {"SERVER_SSH_KEY": "x"}}}}},
        }
        for name, override in cases.items():
            with self.subTest(name):
                self.assert_fails("schema", override)


class ManagerRulesTest(RenderFailureCase):
    def test_fabric_manager_is_required(self):
        self.assert_fails("/global/networking/fabricManager", networking(fabricManager=""))
        self.assert_fails("/global/networking/fabricManager", networking(fabricManager="Netris"))

    def test_k8s_manager_values(self):
        self.assert_fails("/global/networking/k8sManager", networking(k8sManager="INVALID-MANAGER"))

    def test_vmaas_needs_k8s_manager(self):
        self.assert_fails(
            "global.networking.k8sManager is required when VMaaS is enabled; set a registered Kubernetes Manager name",
            networking(k8sManager=""),
        )

    def test_caas_needs_bmaas(self):
        result = helm(services(vmaas=True, bmaas=False, caas=True))
        self.assertNotEqual(result.returncode, 0)
        result = helm(services(vmaas=True, bmaas=False, caas=True), args=("--skip-schema-validation",))
        self.assertIn("CaaS requires BMaaS: set global.services.bmaas.enabled=true", result.stderr)

    def test_provisioning_needs_network_fulfillment(self):
        self.assert_fails(
            "global.networking.provisioningEnabled requires aap.instanceGroups.networkFulfillment.enabled=true",
            {"aap": {"instanceGroups": {"networkFulfillment": {"enabled": False}}}},
        )
        render(
            networking(provisioningEnabled=False),
            {"aap": {"instanceGroups": {"networkFulfillment": {"enabled": False}}}},
        )


class AddressPlanTest(RenderFailureCase):
    @staticmethod
    def plan(overlay, fabric, lb_vip) -> dict:
        return networking(networkClass={"addressPlan": {"overlay": overlay, "fabric": fabric, "lbVip": lb_vip}})

    def test_partial_plan(self):
        self.assert_fails("missing properties", networking(networkClass={"addressPlan": {"overlay": 0.5}}))

    def test_share_values(self):
        for overlay in (0.3, -0.25, 1.25):
            with self.subTest(overlay=overlay):
                self.assert_fails("/global/networking/networkClass/addressPlan/overlay", self.plan(overlay, 0.5, 0.25))

    def test_rules(self):
        cases = [
            ((0.5, 0.25, 0.125), f"{PLAN_PATH} shares must sum to exactly 1 (got 0.875)"),
            (
                (0, 0.75, 0.25),
                f"{PLAN_PATH}.overlay must be greater than 0 exactly when global.networking.k8sManager is set",
            ),
            ((0.5, 0, 0.5), f"{PLAN_PATH}.fabric must be greater than 0 when BMaaS or CaaS is enabled"),
            ((0.5, 0.5, 0), f"{PLAN_PATH}.lbVip must be greater than 0 when CaaS is enabled"),
        ]
        for plan, message in cases:
            with self.subTest(plan=plan):
                self.assert_fails(message, self.plan(*plan))

    def test_overlay_without_k8s_manager(self):
        self.assert_fails(
            f"{PLAN_PATH}.overlay must be greater than 0 exactly when global.networking.k8sManager is set",
            services(vmaas=False, bmaas=True, caas=False),
            networking(k8sManager=""),
            self.plan(0.5, 0.5, 0),
        )

    def test_derived_plans(self):
        cases = [
            ((True, True, True), "example_k8s", (0.5, 0.25, 0.25)),
            ((True, True, False), "example_k8s", (0.5, 0.5, 0)),
            ((True, False, False), "example_k8s", (0.75, 0.25, 0)),
            ((False, True, True), "example_k8s", (0.5, 0.25, 0.25)),
            ((False, True, False), "example_k8s", (0.5, 0.5, 0)),
            ((False, False, False), "example_k8s", (0.75, 0.25, 0)),
            ((False, True, True), "", (0, 0.75, 0.25)),
            ((False, True, False), "", (0, 1, 0)),
            ((False, False, False), "", (0, 1, 0)),
        ]
        for enabled, k8s_manager, plan in cases:
            with self.subTest(services=enabled, k8s_manager=k8s_manager):
                inp = step_input(render(services(*enabled), networking(k8sManager=k8s_manager)))
                self.assertEqual(inp["address_plan"], {"overlay": plan[0], "fabric": plan[1], "lb_vip": plan[2]})
                self.assertFalse(inp["address_plan_explicit"])

    def test_prefix_limits(self):
        cases = [
            # (plan, services, limit)
            ((0.5625, 0.1875, 0.25), (True, True, True), 24),
            ((0.375, 0.375, 0.25), (True, True, True), 25),
            ((0.5, 0.25, 0.25), (True, True, True), 26),
            ((0.5, 0.5, 0), (True, True, False), 27),
        ]
        for plan, enabled, limit in cases:
            with self.subTest(limit=limit):
                values = (services(*enabled), self.plan(*plan))
                ok = networking(networkClass={"defaults": {"subnetIPv4CIDR": f"10.200.0.0/{limit}"}})
                render(*values, ok)
                too_long = networking(networkClass={"defaults": {"subnetIPv4CIDR": f"10.200.0.0/{limit + 1}"}})
                self.assert_fails(
                    f"{DEFAULTS_PATH}.subnetIPv4CIDR prefix /{limit + 1} is longer than "
                    f"the address plan's prefix limit /{limit}",
                    *values,
                    too_long,
                )
        # No k8sManager and BMaaS only: plan 0/1/0, limit /28.
        bmaas_only = (services(False, True, False), networking(k8sManager=""))
        render(*bmaas_only, networking(networkClass={"defaults": {"subnetIPv4CIDR": "10.200.0.0/28"}}))
        self.assert_fails(
            f"{DEFAULTS_PATH}.subnetIPv4CIDR prefix /29 is longer than the address plan's prefix limit /28",
            *bmaas_only,
            networking(networkClass={"defaults": {"subnetIPv4CIDR": "10.200.0.0/29"}}),
        )

    def test_explicit_plan_reaches_step(self):
        inp = step_input(render(self.plan(0.5, 0.25, 0.25)))
        self.assertEqual(inp["address_plan"], {"overlay": 0.5, "fabric": 0.25, "lb_vip": 0.25})
        self.assertTrue(inp["address_plan_explicit"])


class DefaultsTest(RenderFailureCase):
    @staticmethod
    def defaults(**values) -> dict:
        return networking(networkClass={"defaults": values})

    def test_cidrs(self):
        self.assert_fails(
            f"{DEFAULTS_PATH}.virtualNetworkIPv4CIDR must be a canonical IPv4 CIDR (got 10.200.0.1/16)",
            self.defaults(virtualNetworkIPv4CIDR="10.200.0.1/16"),
        )
        self.assert_fails(
            f"{DEFAULTS_PATH}.subnetIPv4CIDR must be inside virtualNetworkIPv4CIDR",
            self.defaults(subnetIPv4CIDR="10.201.0.0/20"),
        )
        self.assert_fails(
            "/global/networking/networkClass/defaults/subnetIPv4CIDR", self.defaults(subnetIPv4CIDR="10.200.0.0")
        )

    def test_rule_shapes(self):
        cases = {
            "ipv4Cidr required": {"protocol": "PROTOCOL_ALL"},
            "tcp needs ports": {"protocol": "PROTOCOL_TCP", "ipv4Cidr": "0.0.0.0/0"},
            "icmp has no ports": {"protocol": "PROTOCOL_ICMP", "ipv4Cidr": "0.0.0.0/0", "portFrom": 1, "portTo": 1},
            "all has no ports": {"protocol": "PROTOCOL_ALL", "ipv4Cidr": "0.0.0.0/0", "portFrom": 1, "portTo": 1},
            "protocol enum": {"protocol": "TCP", "ipv4Cidr": "0.0.0.0/0", "portFrom": 1, "portTo": 1},
            "unknown key": {"protocol": "PROTOCOL_ALL", "ipv4Cidr": "0.0.0.0/0", "ipv6Cidr": "::/0"},
        }
        for name, rule in cases.items():
            with self.subTest(name):
                self.assert_fails(
                    "/global/networking/networkClass/defaults/ingressRules/0", self.defaults(ingressRules=[rule])
                )

    def test_port_order(self):
        self.assert_fails(
            f"{DEFAULTS_PATH}.egressRules[0]: portFrom (80) must be <= portTo (79)",
            self.defaults(
                egressRules=[{"protocol": "PROTOCOL_UDP", "ipv4Cidr": "0.0.0.0/0", "portFrom": 80, "portTo": 79}]
            ),
        )

    def test_step_input(self):
        docs = render(
            networking(
                excludedIpv4Cidrs=["192.0.2.0/24"],
                networkClass={
                    "defaults": {
                        "ingressRules": [
                            {"protocol": "PROTOCOL_TCP", "ipv4Cidr": "0.0.0.0/0", "portFrom": 22, "portTo": 22}
                        ]
                    },
                    "eastWestCapabilities": {"supportsEastWestEthernet": True},
                },
            ),
        )
        self.assertEqual(
            step_input(docs),
            {
                "fabric_manager": "example_fabric",
                "k8s_manager": "example_k8s",
                "defaults": {
                    "virtual_network_ipv4_cidr": "10.200.0.0/16",
                    "subnet_ipv4_cidr": "10.200.0.0/20",
                    "ingress_rules": [
                        {"protocol": "PROTOCOL_TCP", "ipv4_cidr": "0.0.0.0/0", "port_from": 22, "port_to": 22}
                    ],
                    "egress_rules": [{"protocol": "PROTOCOL_ALL", "ipv4_cidr": "0.0.0.0/0"}],
                },
                "east_west_capabilities": {"supports_east_west_ethernet": True},
                "address_plan": {"overlay": 0.5, "fabric": 0.25, "lb_vip": 0.25},
                "address_plan_explicit": False,
                "services": {"vmaas": True, "bmaas": True, "caas": True},
                "excluded_ipv4_cidrs": ["192.0.2.0/24"],
                "provisioning_enabled": True,
            },
        )
        step_cm = find(docs, "ConfigMap", "network-class-step")
        self.assertIn("def run(", step_cm["data"]["network-class-step.py"])

    def test_step_absent_without_fulfillment(self):
        docs = render({"service": {"enabled": False}})
        kinds = {(d["kind"], d["metadata"]["name"]) for d in docs}
        self.assertNotIn(("Job", "create-network-class"), kinds)
        self.assertNotIn(("ValidatingAdmissionPolicy", "osac-osac-managed-resources"), kinds)


class ManifestsTest(unittest.TestCase):
    def test_network_manager_registration_is_deferred_to_its_owner(self):
        names = {
            doc["data"].get("name")
            for doc in render()
            if doc["kind"] == "ConfigMap"
            and (doc["metadata"].get("labels") or {}).get("osac.openshift.io/network-k8s-manager") == "true"
        }
        self.assertEqual(names, set())

    def test_admission_policy(self):
        docs = render()
        policy = find(docs, "ValidatingAdmissionPolicy", "osac-osac-managed-resources")
        binding = find(docs, "ValidatingAdmissionPolicyBinding", "osac-osac-managed-resources")
        operator_sa = next(d for d in docs if d["kind"] == "Deployment" and d["metadata"]["name"] == "osac-operator")
        bmf_sa = next(d for d in docs if d["kind"] == "Deployment" and "bmf" in d["metadata"]["name"])
        identities = policy["spec"]["variables"][0]["expression"]
        for sa in (
            "hub-access",
            operator_sa["spec"]["template"]["spec"]["serviceAccountName"],
            bmf_sa["spec"]["template"]["spec"]["serviceAccountName"],
            # AAP job pods remove the finalizers their delete playbooks own.
            "osac-sa",
        ):
            self.assertIn(f'"system:serviceaccount:osac:{sa}"', identities)
        self.assertEqual(
            policy["spec"]["validations"][0]["message"],
            "OSAC manages this resource; change it through the fulfillment API",
        )
        resources = policy["spec"]["matchConstraints"]["resourceRules"][0]["resources"]
        for name in (
            "networkclasses",
            "networkattachments/status",
            "networksegmentclaims",
            "subnets",
            "fabricdomains/status",
        ):
            self.assertIn(name, resources)
        self.assertEqual(binding["spec"]["policyName"], policy["metadata"]["name"])
        self.assertEqual(binding["spec"]["validationActions"], ["Deny"])
        self.assertEqual(
            binding["spec"]["matchResources"]["namespaceSelector"]["matchLabels"],
            {"kubernetes.io/metadata.name": "osac"},
        )

    def test_admission_policy_follows_service_account_overrides(self):
        docs = render(
            {
                "operator": {"serviceAccount": {"name": "custom-operator"}},
                "bmf": {"serviceAccount": {"name": "custom-bmf"}},
            }
        )
        identities = find(docs, "ValidatingAdmissionPolicy", "osac-osac-managed-resources")["spec"]["variables"][0][
            "expression"
        ]
        self.assertIn('"system:serviceaccount:osac:custom-operator"', identities)
        self.assertIn('"system:serviceaccount:osac:custom-bmf"', identities)

    def test_hub_access_can_manage_network_classes(self):
        docs = render({"hubAccess": {"enabled": True}})
        role = find(docs, "ClusterRole", "osac-hub-access")
        self.assertIn("networkclasses", role["rules"][0]["resources"])
        self.assertIn("create", role["rules"][0]["verbs"])


class AapSettingsTest(unittest.TestCase):
    def test_network_fulfillment_settings_written_verbatim(self):
        ca = "-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n"
        docs = render(
            networking(fabricManager="acme"),
            {
                "aap": {
                    "instanceGroups": {
                        "networkFulfillment": {
                            "config": {"ACME_FABRIC_URL": "https://fabric.example.com", "ACME_FABRIC_CA": ca},
                            "secret": {"ACME_FABRIC_TOKEN": "s3cr3t"},
                        }
                    }
                }
            }
        )
        cm = find(docs, "ConfigMap", "network-fulfillment-ig")
        self.assertEqual(cm["data"], {"ACME_FABRIC_URL": "https://fabric.example.com", "ACME_FABRIC_CA": ca})
        secret = find(docs, "Secret", "network-fulfillment-ig")
        self.assertEqual(set(secret["data"]), {"ACME_FABRIC_TOKEN"})
        for doc in docs:
            if doc["kind"] in ("ConfigMap", "Secret"):
                self.assertNotIn("NETWORK_CLASS", (doc.get("data") or {}))
                self.assertNotIn("NETWORK_STEPS_COLLECTION", (doc.get("data") or {}))

    def test_setting_names(self):
        result = helm({"aap": {"instanceGroups": {"networkFulfillment": {"config": {"lower_case": "x"}}}}})
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("lower_case", result.stderr)

    def test_schema_advertises_free_form_settings(self):
        schema = json.loads((CHART / "values.schema.json").read_text())
        nf = schema["properties"]["aap"]["properties"]["instanceGroups"]["properties"]["networkFulfillment"]
        self.assertIn("network-fulfillment-ig", nf["description"])
        settings = schema["$defs"]["managerSettings"]
        for key in ("config", "secret"):
            self.assertEqual(nf["properties"][key]["$ref"], "#/$defs/managerSettings")
        self.assertEqual(settings["additionalProperties"], {"type": "string"})
        self.assertNotIn("properties", settings)
        self.assertNotIn("aapNetrisConfig", schema["$defs"])


class SubchartPlumbingTest(unittest.TestCase):
    """Networking installation settings reach the operator, BMF and fulfillment service."""

    @staticmethod
    def deployment_env(docs, name_part: str) -> dict:
        envs = {}
        for doc in docs:
            if doc["kind"] == "Deployment" and name_part in doc["metadata"]["name"]:
                for c in doc["spec"]["template"]["spec"]["containers"]:
                    envs.update({e["name"]: e.get("value") for e in c.get("env", [])})
        return envs

    @staticmethod
    def fulfillment_args(docs) -> list[str]:
        args = []
        for doc in docs:
            if doc["kind"] == "Deployment" and doc["metadata"]["name"].startswith("fulfillment-"):
                for c in doc["spec"]["template"]["spec"]["containers"]:
                    args.extend(c.get("args") or [])
                    args.extend(c.get("command") or [])
        return args

    def test_provisioning_enabled(self):
        for enabled in (True, False):
            with self.subTest(enabled=enabled):
                docs = render(
                    networking(provisioningEnabled=enabled, excludedIpv4Cidrs=["192.0.2.0/24", "198.51.100.0/24"])
                )
                value = str(enabled).lower()
                # The BMF subchart still owns a separate legacy toggle; the
                # shared setting reaches the core Operator and Fulfillment only.
                self.assertEqual(self.deployment_env(docs, "osac-operator").get("OSAC_PROVISIONING_ENABLED"), value)
                args = self.fulfillment_args(docs)
                self.assertIn(f"--networking-provisioning-enabled={value}", args)
                self.assertIn("--networking-excluded-ipv4-cidrs=192.0.2.0/24,198.51.100.0/24", args)


if __name__ == "__main__":
    unittest.main()
