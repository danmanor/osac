from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from ansible.module_utils.common.arg_spec import ArgumentSpecValidator


AAP_ROOT = Path(__file__).resolve().parents[2]
ARGUMENT_SPECS = (
    AAP_ROOT
    / "collections/ansible_collections/osac/templates/roles/netris/meta/argument_specs.yaml"
)


def move_argument_spec():
    return yaml.safe_load(ARGUMENT_SPECS.read_text(encoding="utf-8"))["argument_specs"][
        "move_network_attachment"
    ]["options"]


@pytest.mark.parametrize("offboard", [False, True])
@pytest.mark.parametrize("include_subnet_ref", [False, True])
def test_netris_accepts_shared_move_attachment_payload(offboard, include_subnet_ref):
    payload = {
        "host_name": "test-host",
        "logical_interface_name": "eth9",
        "from_vnet_name": "tenant-vnet" if offboard else "bm-provisioning",
        "to_vnet_name": "bm-provisioning" if offboard else "tenant-vnet",
    }
    if include_subnet_ref:
        payload["subnet_ref"] = "11111111-2222-4333-8444-555555555555"

    result = ArgumentSpecValidator(move_argument_spec()).validate(
        {"network_attachment": payload}, validate_role_argument_spec=True
    )

    assert not result.error_messages, result.error_messages


def test_netris_move_attachment_still_rejects_unknown_fields():
    result = ArgumentSpecValidator(move_argument_spec()).validate(
        {
            "network_attachment": {
                "host_name": "test-host",
                "logical_interface_name": "eth9",
                "unexpected_field": "invalid",
            }
        },
        validate_role_argument_spec=True,
    )

    assert result.error_messages
    assert "unexpected_field" in " ".join(result.error_messages)
