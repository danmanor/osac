#!/usr/bin/env python3
"""Verify the EVPN installer values contract renders as expected."""

from __future__ import annotations

import os
import pathlib
import subprocess
import tempfile
from typing import Any

import yaml


ROOT = pathlib.Path(__file__).resolve().parents[1]
DEPS_CHART = ROOT / "charts/osac-deps"
INFRA_CHART = ROOT / "charts/osac-infra"


def render(release: str, chart: pathlib.Path, *args: str) -> list[dict[str, Any]]:
    result = subprocess.run(
        [
            os.environ.get("HELM_BIN", "helm"),
            "template",
            release,
            str(chart),
            "--namespace",
            "osac-infra",
            *args,
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode:
        raise RuntimeError(f"Helm render failed: {result.stderr}")
    return [
        item
        for item in yaml.safe_load_all(result.stdout)
        if isinstance(item, dict)
    ]


def expect_render_failure(chart: pathlib.Path, values_file: pathlib.Path) -> None:
    result = subprocess.run(
        [
            os.environ.get("HELM_BIN", "helm"),
            "template",
            "osac-infra",
            str(chart),
            "--namespace",
            "osac-infra",
            "--values",
            str(values_file),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode != 0, "invalid EVPN values unexpectedly rendered"


def named(
    resources: list[dict[str, Any]], kind: str, name: str
) -> dict[str, Any]:
    matches = [
        item
        for item in resources
        if item.get("kind") == kind and item.get("metadata", {}).get("name") == name
    ]
    assert len(matches) == 1, f"expected one {kind}/{name}, got {len(matches)}"
    return matches[0]


def absent(resources: list[dict[str, Any]], kind: str, name: str) -> None:
    assert not any(
        item.get("kind") == kind and item.get("metadata", {}).get("name") == name
        for item in resources
    ), f"unexpected {kind}/{name} resource"


def config_data(resources: list[dict[str, Any]]) -> dict[str, str]:
    return named(resources, "ConfigMap", "osac-infra-evpn-config")["data"]


def parse_documents(value: str) -> list[dict[str, Any]]:
    return [item for item in yaml.safe_load_all(value) if isinstance(item, dict)]


def write_values(directory: pathlib.Path, name: str, values: dict[str, Any]) -> pathlib.Path:
    path = directory / name
    path.write_text(yaml.safe_dump(values, sort_keys=False), encoding="utf-8")
    return path


def node(
    name: str,
    vtep_ip: str,
    uplinks: list[dict[str, Any]],
    asn: int | None = None,
) -> dict[str, Any]:
    value: dict[str, Any] = {"name": name, "vtepIP": vtep_ip, "uplinks": uplinks}
    if asn is not None:
        value["asn"] = asn
    return value


def uplink(
    interface: str,
    peer_address: str,
    peer_asn: int = 4200300002,
    address: str | None = None,
) -> dict[str, Any]:
    value: dict[str, Any] = {
        "interface": interface,
        "peerAddress": peer_address,
        "peerASN": peer_asn,
    }
    if address is not None:
        value["address"] = address
    return value


def base_values(nodes: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "evpn": {
            "enabled": True,
            "nmstate": {"install": True, "channel": "stable"},
            "configureClusterNetwork": True,
            "vtepCIDR": "10.200.255.0/24",
            "bgp": {
                "asn": 4209999999,
                "acceptPrefixes": ["10.200.0.0/16"],
                "waitForEstablished": True,
                "timeoutSeconds": 900,
            },
            "nodes": nodes,
        }
    }


def test_defaults() -> None:
    deps = render("osac-deps", DEPS_CHART)
    infra = render("osac-infra", INFRA_CHART)
    assert not any(
        item.get("metadata", {}).get("namespace") == "openshift-nmstate"
        for item in deps
    ), "disabled EVPN must not render NMState operator resources"
    absent(infra, "ConfigMap", "osac-infra-evpn-config")
    absent(infra, "Job", "osac-infra-configure-evpn")


def test_single_node() -> None:
    values = base_values(
        [node("worker-0", "10.200.255.1", [uplink("ens6", "169.254.133.1", address="169.254.133.2/31")])]
    )
    with tempfile.TemporaryDirectory(prefix="evpn-values-") as temp:
        values_file = write_values(pathlib.Path(temp), "single.yaml", values)
        deps = render("osac-deps", DEPS_CHART, "--values", str(values_file))
        infra = render("osac-infra", INFRA_CHART, "--values", str(values_file))

    subscription = named(deps, "Subscription", "kubernetes-nmstate-operator")
    assert subscription["metadata"]["namespace"] == "openshift-nmstate"
    named(infra, "ConfigMap", "osac-infra-evpn-config")
    named(infra, "Job", "osac-infra-configure-evpn")

    data = config_data(infra)
    nncp = named(parse_documents(data["nncp.yaml"]), "NodeNetworkConfigurationPolicy", "osac-evpn-worker-0")
    interfaces = nncp["spec"]["desiredState"]["interfaces"]
    ethernet = next(interface for interface in interfaces if interface["name"] == "ens6")
    assert ethernet["ipv4"]["address"] == [{"ip": "169.254.133.2", "prefix-length": 31}]
    vtep_interface = next(interface for interface in interfaces if interface["name"] == "evpn-vtep0")
    assert vtep_interface["ipv4"]["address"] == [{"ip": "10.200.255.1", "prefix-length": 32}]

    vtep = named(parse_documents(data["vtep.yaml"]), "VTEP", "tenant-vtep")
    assert vtep["spec"]["cidrs"] == ["10.200.255.0/24"]
    route_advertisements = named(
        parse_documents(data["routeadvertisements.yaml"]), "RouteAdvertisements", "osac-evpn"
    )
    selector = route_advertisements["spec"]["networkSelectors"][0][
        "clusterUserDefinedNetworkSelector"
    ]["networkSelector"]["matchLabels"]
    assert selector == {"evpn": "true"}

    frr = parse_documents(data["frr.yaml"])
    underlay = named(frr, "FRRConfiguration", "osac-evpn-worker-0")
    router = underlay["spec"]["bgp"]["routers"][0]
    assert router["asn"] == 4209999999 and isinstance(router["asn"], int)
    assert "sourceaddress" in router["neighbors"][0]
    assert "e+09" not in str(infra), "large ASNs must not render as float notation"
    assert data["peers.txt"].splitlines() == ["worker-0 169.254.133.1"]


def test_multiple_nodes() -> None:
    values = base_values(
        [
            node("worker-0", "10.200.255.1", [uplink("ens6", "169.254.133.1")]),
            node(
                "worker-1",
                "10.200.255.2",
                [uplink("ens6", "169.254.134.1"), uplink("ens7", "169.254.135.1")],
                asn=4209999998,
            ),
        ]
    )
    with tempfile.TemporaryDirectory(prefix="evpn-values-") as temp:
        values_file = write_values(pathlib.Path(temp), "multiple.yaml", values)
        infra = render("osac-infra", INFRA_CHART, "--values", str(values_file))

    data = config_data(infra)
    nncps = parse_documents(data["nncp.yaml"])
    assert {item["metadata"]["name"] for item in nncps} == {
        "osac-evpn-worker-0",
        "osac-evpn-worker-1",
    }
    frr = parse_documents(data["frr.yaml"])
    second = named(frr, "FRRConfiguration", "osac-evpn-worker-1")
    assert second["spec"]["bgp"]["routers"][0]["asn"] == 4209999998
    raw = named(frr, "FRRConfiguration", "osac-evpn-raw-worker-1")
    raw_config = raw["spec"]["raw"]["rawConfig"]
    assert "neighbor 169.254.134.1 activate" in raw_config
    assert "neighbor 169.254.135.1 activate" in raw_config
    assert len(data["peers.txt"].splitlines()) == 3


def test_uplink_without_address() -> None:
    values = base_values([node("worker-0", "10.200.255.1", [uplink("ens6", "169.254.133.1")])])
    with tempfile.TemporaryDirectory(prefix="evpn-values-") as temp:
        values_file = write_values(pathlib.Path(temp), "no-address.yaml", values)
        infra = render("osac-infra", INFRA_CHART, "--values", str(values_file))

    data = config_data(infra)
    nncp = named(parse_documents(data["nncp.yaml"]), "NodeNetworkConfigurationPolicy", "osac-evpn-worker-0")
    interfaces = nncp["spec"]["desiredState"]["interfaces"]
    assert all(interface["name"] != "ens6" for interface in interfaces)
    underlay = named(parse_documents(data["frr.yaml"]), "FRRConfiguration", "osac-evpn-worker-0")
    assert "sourceaddress" not in underlay["spec"]["bgp"]["routers"][0]["neighbors"][0]


def test_nmstate_install_disabled() -> None:
    values = base_values([node("worker-0", "10.200.255.1", [uplink("ens6", "169.254.133.1")])])
    values["evpn"]["nmstate"]["install"] = False
    with tempfile.TemporaryDirectory(prefix="evpn-values-") as temp:
        values_file = write_values(pathlib.Path(temp), "no-subscription.yaml", values)
        deps = render("osac-deps", DEPS_CHART, "--values", str(values_file))
        infra = render("osac-infra", INFRA_CHART, "--values", str(values_file))

    absent(deps, "Subscription", "kubernetes-nmstate-operator")
    named(infra, "Job", "osac-infra-configure-evpn")


def test_invalid_values() -> None:
    valid_node = node("worker-0", "10.200.255.1", [uplink("ens6", "169.254.133.1")])
    invalid: list[dict[str, Any]] = []

    empty_vtep = base_values([valid_node])
    empty_vtep["evpn"]["vtepCIDR"] = ""
    invalid.append(empty_vtep)

    no_nodes = base_values([])
    invalid.append(no_nodes)

    duplicate_vtep = base_values(
        [
            valid_node,
            node("worker-1", "10.200.255.1", [uplink("ens7", "169.254.134.1")]),
        ]
    )
    invalid.append(duplicate_vtep)

    duplicate_name = base_values(
        [
            valid_node,
            node("worker-0", "10.200.255.2", [uplink("ens7", "169.254.134.1")]),
        ]
    )
    invalid.append(duplicate_name)

    duplicate_peer = base_values(
        [
            node(
                "worker-0",
                "10.200.255.1",
                [uplink("ens6", "169.254.133.1"), uplink("ens7", "169.254.133.1")],
            )
        ]
    )
    invalid.append(duplicate_peer)

    invalid_asn = base_values([valid_node])
    invalid_asn["evpn"]["bgp"]["asn"] = 4294967295
    invalid.append(invalid_asn)

    missing_peer_asn = base_values(
        [node("worker-0", "10.200.255.1", [{"interface": "ens6", "peerAddress": "169.254.133.1"}])]
    )
    invalid.append(missing_peer_asn)

    with tempfile.TemporaryDirectory(prefix="evpn-invalid-") as temp:
        directory = pathlib.Path(temp)
        for index, values in enumerate(invalid):
            values_file = write_values(directory, f"invalid-{index}.yaml", values)
            expect_render_failure(INFRA_CHART, values_file)


def main() -> None:
    test_defaults()
    test_single_node()
    test_multiple_nodes()
    test_uplink_without_address()
    test_nmstate_install_disabled()
    test_invalid_values()
    print("EVPN host networking Helm render contract passed.")


if __name__ == "__main__":
    main()
