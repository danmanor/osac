from __future__ import annotations

from uuid import uuid4

from tests.e2e.core.grpc_client import GRPCClient
from tests.e2e.core.helpers import wait_for_subnet_cr, wait_for_subnet_deletion, wait_for_subnet_ready
from tests.e2e.core.k8s_client import K8sClient
from tests.e2e.core.runner import poll_until


def create_and_wait_for_subnet(
    grpc: GRPCClient, k8s_hub_client: K8sClient, virtual_network_id: str, ipv4_cidr: str
) -> tuple[str, str]:
    subnet_name = f"test-subnet-{uuid4().hex[:8]}"
    subnet_id = grpc.create_subnet(name=subnet_name, virtual_network=virtual_network_id, ipv4_cidr=ipv4_cidr)
    subnet_cr_name: str | None = None
    try:
        subnet_cr_name = wait_for_subnet_cr(k8s=k8s_hub_client, uuid=subnet_id)
        assert subnet_id in grpc.list_subnet_ids()
        subnet: dict = grpc.get_subnet(subnet_id=subnet_id)
        assert subnet["object"]["metadata"]["name"] == subnet_name
        wait_for_subnet_ready(k8s=k8s_hub_client, name=subnet_cr_name)
        return subnet_id, subnet_cr_name
    except Exception:
        grpc.delete_subnet(subnet_id=subnet_id)
        if subnet_cr_name is not None:
            wait_for_subnet_deletion(k8s=k8s_hub_client, name=subnet_cr_name)
        raise


def delete_and_wait_for_subnet(
    grpc: GRPCClient, k8s_hub_client: K8sClient, subnet_id: str, subnet_cr_name: str
) -> None:
    grpc.delete_subnet(subnet_id=subnet_id)
    wait_for_subnet_deletion(k8s=k8s_hub_client, name=subnet_cr_name)
    poll_until(
        fn=lambda: subnet_id not in grpc.list_subnet_ids(),
        until=lambda v: v is True,
        retries=30,
        delay=5,
        description=f"Subnet {subnet_id} removal from API",
    )
