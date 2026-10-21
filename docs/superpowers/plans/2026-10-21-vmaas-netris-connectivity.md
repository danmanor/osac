# Netris VMaaS Connectivity E2E

## Goal and constraints

Add Netris VMaaS connectivity coverage modeled on the BMaaS networking suite,
plus a manual full-install workflow that calls the reusable workflow from
osac-test-infra PR #622.

The VMaaS profile allows one Subnet per VirtualNetwork and each VM has one
network attachment. The suite therefore tests same-Subnet reachability and
cross-VirtualNetwork isolation; it does not test same-VirtualNetwork,
cross-Subnet routing. NAT egress and ExternalIP ingress are gated until the
lab's Netris route and return policy are configured.

## Completed work

- [x] Added a VM guest helper using `virtctl ssh` and the VM cluster kubeconfig,
  with bounded commands and explicit handling for probe, transport, timeout,
  and missing-binary failures.
- [x] Added unit coverage for guest command construction, kubeconfig selection,
  positive and negative network probes, malformed targets, curl status parsing,
  transport failures, and ExternalIP SSH identity selection.
- [x] Added five VMaaS E2E cases: same-Subnet ARP and ping, cross-VirtualNetwork
  ARP and ping isolation, management endpoint isolation, NAT egress, and
  ExternalIP SSH ingress.
- [x] The suite provisions two VirtualNetworks with one Subnet each and three
  VMs with one attachment each. It cleans up created resources in dependency
  order and continues cleanup if an individual external resource deletion
  fails.
- [x] Added reusable and manual caller workflows pinned to the current PR #622
  commit, registered the manual-only suite in E2E policy and PR routing, and
  documented the suite prerequisites and coverage boundary.
- [x] Added the suite to the E2E touched-area map and integration testing matrix.

## Validation

- [x] Focused unit tests: `uv run pytest -n 0 tests/unit/test_vmaas_guest_ssh.py tests/unit/test_bmi_ssh.py`.
- [x] E2E collection: `uv run pytest --collect-only -n 0 tests/e2e/vmaas/regression/networking`.
- [x] Ruff checks and format check for changed Python files.
- [x] Applicable pre-commit hooks and `git diff --check`.
- [x] `actionlint` validation for both new workflows and `e2e-pr.yml`.
- [ ] Live Netris VMaaS execution; requires the test-infra environment and
  external return-path configuration.
