from __future__ import annotations

from uuid import uuid4

import pytest

from tests.e2e.core.grpc_client import GRPCClient
from tests.e2e.core.helpers import (
    wait_for_subnet_cr,
    wait_for_subnet_deletion,
    wait_for_subnet_ready,
    wait_for_virtual_network_cr,
    wait_for_virtual_network_deletion,
    wait_for_virtual_network_ready,
)
from tests.e2e.core.k8s_client import K8sClient
from tests.e2e.core.runner import poll_until

pytestmark = pytest.mark.sanity


def test_virtual_network_lifecycle(grpc: GRPCClient, k8s_hub_client: K8sClient) -> None:
    vn_name: str = f"test-vnet-{uuid4().hex[:8]}"
    vn_id: str | None = None
    vn_cr_name: str | None = None
    subnet_id: str | None = None
    subnet_cr_name: str | None = None

    try:
        vn_id = grpc.create_virtual_network(name=vn_name, ipv4_cidr="10.100.0.0/16")
        vn_cr_name = wait_for_virtual_network_cr(k8s=k8s_hub_client, uuid=vn_id)

        assert vn_id in grpc.list_virtual_network_ids()

        vn: dict = grpc.get_virtual_network(vn_id=vn_id)
        assert vn["object"]["metadata"]["name"] == vn_name

        wait_for_virtual_network_ready(k8s=k8s_hub_client, name=vn_cr_name)

        subnet_name: str = f"test-subnet-{uuid4().hex[:8]}"
        subnet_id = grpc.create_subnet(name=subnet_name, virtual_network=vn_id, ipv4_cidr="10.100.1.0/24")
        subnet_cr_name = wait_for_subnet_cr(k8s=k8s_hub_client, uuid=subnet_id)

        assert subnet_id in grpc.list_subnet_ids()

        subnet: dict = grpc.get_subnet(subnet_id=subnet_id)
        assert subnet["object"]["metadata"]["name"] == subnet_name

        wait_for_subnet_ready(k8s=k8s_hub_client, name=subnet_cr_name)

        grpc.delete_subnet(subnet_id=subnet_id)
        wait_for_subnet_deletion(k8s=k8s_hub_client, name=subnet_cr_name)
        poll_until(
            fn=lambda: subnet_id not in grpc.list_subnet_ids(),
            until=lambda v: v is True,
            retries=30,
            delay=5,
            description=f"Subnet {subnet_id} removal from API",
        )
        subnet_id = None
        subnet_cr_name = None

        grpc.delete_virtual_network(vn_id=vn_id)
        wait_for_virtual_network_deletion(k8s=k8s_hub_client, name=vn_cr_name)
        poll_until(
            fn=lambda: vn_id not in grpc.list_virtual_network_ids(),
            until=lambda v: v is True,
            retries=30,
            delay=5,
            description=f"VirtualNetwork {vn_id} removal from API",
        )
        vn_id = None
        vn_cr_name = None
    finally:
        if subnet_id is not None:
            grpc.delete_subnet(subnet_id=subnet_id)
            if subnet_cr_name is not None:
                wait_for_subnet_deletion(k8s=k8s_hub_client, name=subnet_cr_name)
        if vn_id is not None:
            grpc.delete_virtual_network(vn_id=vn_id)
            if vn_cr_name is not None:
                wait_for_virtual_network_deletion(k8s=k8s_hub_client, name=vn_cr_name)
