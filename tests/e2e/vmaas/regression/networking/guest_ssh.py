from __future__ import annotations

import ipaddress
import logging
import os
import re
import shlex
import subprocess

log = logging.getLogger(__name__)

_PROBE_EXIT_SENTINEL = "__OSAC_PROBE_EXIT_STATUS__"
_PROBE_EXIT_RE = re.compile(rf"(?m)^{_PROBE_EXIT_SENTINEL}=(\d+)$")
_HTTP_STATUS_SENTINEL = "__OSAC_HTTP_STATUS__"
_HTTP_STATUS_RE = re.compile(rf"(?m)^{_HTTP_STATUS_SENTINEL}(\d{{3}})$")


def _combined_output(stdout: str | bytes | None, stderr: str | bytes | None) -> str:
    def as_text(value: str | bytes | None) -> str:
        if isinstance(value, bytes):
            return value.decode(errors="replace")
        return value or ""

    return (as_text(stdout).strip() + "\n" + as_text(stderr).strip()).strip()


def _ssh_user() -> str:
    return os.environ.get("OSAC_VM_SSH_USER", "fedora").strip() or "fedora"


def ssh_guest_unchecked(
    *, vm_namespace: str, vm_name: str, identity_file: str, command: str, timeout: int = 30
) -> tuple[str, int]:
    """Run a command through KubeVirt's SSH port-forward and return output/status."""
    vm_kubeconfig = os.environ.get("OSAC_VM_KUBECONFIG", "").strip()
    if not vm_kubeconfig:
        raise RuntimeError("OSAC_VM_KUBECONFIG must identify the VM cluster kubeconfig")

    target = f"{_ssh_user()}@vm/{vm_name}/{vm_namespace}"
    args = ["virtctl", "ssh", "--identity-file", identity_file, "--command", command, target]
    child_env = os.environ.copy()
    child_env["KUBECONFIG"] = vm_kubeconfig
    try:
        result = subprocess.run(args, capture_output=True, text=True, timeout=timeout, check=False, env=child_env)
    except subprocess.TimeoutExpired as exc:
        output = _combined_output(exc.stdout, exc.stderr)
        raise RuntimeError(f"virtctl SSH to {vm_namespace}/{vm_name} timed out after {timeout}s: {output}") from exc
    except OSError as exc:
        raise RuntimeError(f"failed to start virtctl SSH to {vm_namespace}/{vm_name}: {exc}") from exc

    output = _combined_output(result.stdout, result.stderr)
    log.info(
        "VM guest command (%s/%s): %s; virtctl_rc=%d; output=%r",
        vm_namespace,
        vm_name,
        command,
        result.returncode,
        output,
    )
    return output, result.returncode


def ssh_guest(
    *, vm_namespace: str, vm_name: str, identity_file: str, command: str, timeout: int = 30
) -> tuple[str, int]:
    """Run a command in a VM through its cluster's KubeVirt SSH port-forward."""
    return ssh_guest_unchecked(
        vm_namespace=vm_namespace, vm_name=vm_name, identity_file=identity_file, command=command, timeout=timeout
    )


def _probe_connectivity(vm_namespace: str, vm_name: str, identity_file: str, command: str, timeout: int = 30) -> bool:
    wrapped_command = (
        f'{command}; probe_rc=$?; printf \'\\n{_PROBE_EXIT_SENTINEL}=%s\\n\' "$probe_rc"; exit "$probe_rc"'
    )
    output, virtctl_rc = ssh_guest_unchecked(
        vm_namespace=vm_namespace,
        vm_name=vm_name,
        identity_file=identity_file,
        command=wrapped_command,
        timeout=timeout,
    )
    matches = _PROBE_EXIT_RE.findall(output)
    if len(matches) != 1:
        raise RuntimeError(
            f"VM guest connectivity probe from {vm_namespace}/{vm_name} did not complete "
            f"(virtctl_rc={virtctl_rc}, output={output!r})"
        )

    probe_rc = int(matches[0])
    if virtctl_rc != probe_rc:
        raise RuntimeError(
            f"VM guest connectivity probe from {vm_namespace}/{vm_name} reported command exit {probe_rc}, "
            f"but virtctl exited {virtctl_rc} (output={output!r})"
        )
    if probe_rc not in (0, 1):
        raise RuntimeError(
            f"VM guest connectivity probe from {vm_namespace}/{vm_name} failed with unexpected command exit "
            f"{probe_rc} (output={output!r})"
        )
    return probe_rc == 0


def _ipv4_target(target_ip: str) -> str:
    try:
        return str(ipaddress.IPv4Address(target_ip))
    except ipaddress.AddressValueError as exc:
        raise ValueError(f"{target_ip!r} is not a valid IPv4 address") from exc


def arping(vm_namespace: str, vm_name: str, identity_file: str, target_ip: str, count: int = 3) -> bool:
    ip = _ipv4_target(target_ip)
    return _probe_connectivity(vm_namespace, vm_name, identity_file, f"arping -c {count} {shlex.quote(ip)}", timeout=30)


def ping(vm_namespace: str, vm_name: str, identity_file: str, target_ip: str, count: int = 3, wait: int = 3) -> bool:
    ip = _ipv4_target(target_ip)
    return _probe_connectivity(
        vm_namespace, vm_name, identity_file, f"ping -c {count} -W {wait} {shlex.quote(ip)}", timeout=30
    )


def curl_status(vm_namespace: str, vm_name: str, identity_file: str, timeout: int = 15) -> int:
    command = (
        f"curl -sS -o /dev/null -w '\\n{_HTTP_STATUS_SENTINEL}%{{http_code}}\\n' "
        f"--connect-timeout {timeout} https://quay.io"
    )
    output, virtctl_rc = ssh_guest_unchecked(
        vm_namespace=vm_namespace, vm_name=vm_name, identity_file=identity_file, command=command, timeout=timeout + 30
    )
    if virtctl_rc != 0:
        log.warning("VM guest curl via %s/%s exited %d: %s", vm_namespace, vm_name, virtctl_rc, output)
        return 0
    matches = _HTTP_STATUS_RE.findall(output)
    if len(matches) != 1:
        log.warning("VM guest curl via %s/%s returned no unique HTTP status: %r", vm_namespace, vm_name, output)
        return 0
    return int(matches[0])


def ssh_via_external_ip(external_ip: str, identity_file: str, command: str = "hostname", timeout: int = 15) -> str:
    host = _ipv4_target(external_ip)
    args = [
        "ssh",
        "-o",
        f"ConnectTimeout={timeout}",
        "-o",
        "BatchMode=yes",
        "-o",
        "IdentitiesOnly=yes",
        "-o",
        "StrictHostKeyChecking=no",
        "-o",
        "UserKnownHostsFile=/dev/null",
        "-i",
        identity_file,
        f"{_ssh_user()}@{host}",
        command,
    ]
    result = subprocess.run(args, capture_output=True, text=True, timeout=timeout + 10, check=True)
    output = _combined_output(result.stdout, result.stderr)
    log.info("VM ExternalIP SSH (%s): %s; ssh_rc=%d; output=%r", host, command, result.returncode, output)
    return result.stdout.strip()
