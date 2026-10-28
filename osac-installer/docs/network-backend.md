# Networking Configuration

OSAC networking follows the unified networking design (OSAC-1433): the
fulfillment service stores networking resources, the OSAC operator runs the
network managers' tasks through AAP, and one NetworkClass selects the managers.
This page describes the installer side.

## Helm values

```yaml
global:
  networking:
    provisioningEnabled: true      # fixed at installation
    excludedIpv4Cidrs: []          # provider infrastructure ranges
    fabricManager: acme_fabric     # required: the Fabric Manager registration name
    k8sManager: cudn_evpn          # "" or cudn_evpn; required with VMaaS
    networkClass:
      defaults:
        virtualNetworkIPv4CIDR: 10.200.0.0/16
        subnetIPv4CIDR: 10.200.0.0/20
        ingressRules: []
        egressRules:
        - {protocol: PROTOCOL_ALL, ipv4Cidr: 0.0.0.0/0}
      eastWestCapabilities:
        supportsEastWestEthernet: false
      # addressPlan: {overlay: 0.5, fabric: 0.25, lbVip: 0.25}
aap:
  instanceGroups:
    networkFulfillment:
      enabled: true
      config: {}   # ConfigMap network-fulfillment-ig, written verbatim
      secret: {}   # Secret network-fulfillment-ig, written verbatim
```

- `provisioningEnabled`: with `false`, OSAC runs no networking jobs and
  networking resources become READY without provider work. The fulfillment
  service stores the value at its first start and refuses to start when a
  later rollout changes it.
- `excludedIpv4Cidrs`: canonical IPv4 CIDRs the provider's infrastructure uses
  (management, provisioning and node networks, and the DNS resolvers given to
  workloads). VirtualNetwork and ExternalIPPool CIDRs may not overlap them.
- `fabricManager`: the `name` in the provider's Fabric Manager registration.
  There is no default.
- `k8sManager`: the OSAC Kubernetes Manager. The operator chart always creates
  its registration (`osac-network-k8s-manager-cudn-evpn-<hash>`).
- `networkClass.defaults` and `networkClass.eastWestCapabilities` become the
  NetworkClass and cannot change after installation. Rules use SecurityGroup
  rule fields: TCP and UDP rules need `portFrom` and `portTo`; ICMP and ALL
  rules have no ports; `ipv4Cidr` is required.
- `networkClass.addressPlan`: optional shares of each Subnet for the
  Kubernetes Manager (`overlay`), the Fabric Manager (`fabric`) and CaaS
  ingress reservations (`lbVip`). Each is a multiple of 0.0625 and they sum to
  1. Without it the installer derives the plan from the enabled services and
  `k8sManager`:

  | Enabled services | With `k8sManager` | Without |
  |---|---|---|
  | VMaaS, BMaaS, CaaS | 0.5 / 0.25 / 0.25 | not allowed |
  | VMaaS, BMaaS | 0.5 / 0.5 / 0 | not allowed |
  | VMaaS | 0.75 / 0.25 / 0 | not allowed |
  | BMaaS, CaaS | 0.5 / 0.25 / 0.25 | 0 / 0.75 / 0.25 |
  | BMaaS | 0.5 / 0.5 / 0 | 0 / 1 / 0 |
  | None of them | 0.75 / 0.25 / 0 | 0 / 1 / 0 |

  The plan sets a prefix limit for `subnetIPv4CIDR`: from /28 (one non-zero
  share) down to /24 (shares in sixteenths).
- `aap.instanceGroups.networkFulfillment.config` / `.secret`: the managers'
  settings and credentials. Each key starts with the manager's registration
  name in upper case (`NETRIS_CONTROLLER_URL`, `NETRIS_PASSWORD`,
  `CUDN_EVPN_DNS_RESOLVERS`, ...); values may span several lines (CA bundles).
  Required with `provisioningEnabled: true`.
- `aap.configAsCode.eeImage`: the execution environment of every OSAC instance
  group; with a provider Fabric Manager, the provider's image built on
  `aap.bootstrap.image`.

## Manager registration

Manager registration ConfigMaps and implementations are owned by the
corresponding manager teams. The installer does not ship or apply
manager-specific registrations. When `provisioningEnabled` is `true`, install
the required registrations and provider configuration using the manager's
documented procedure. With provider networking disabled, the operator does not
require manager registrations for networking resources to become READY.

## Checks during a rollout

Render time (`helm template`/`install`/`upgrade`) rejects values that break
the schema or these rules, naming the field:

- `fabricManager` is set; VMaaS needs `k8sManager`;
  `provisioningEnabled` needs `aap.instanceGroups.networkFulfillment.enabled`;
- the address plan in force: shares, sum, `overlay` > 0 exactly with
  `k8sManager`, `fabric` > 0 with BMaaS or CaaS, `lbVip` > 0 with CaaS;
- canonical default CIDRs, the Subnet inside the VirtualNetwork, the prefix
  limit, and `portFrom <= portTo`.

The installer's NetworkClass step (post-install/post-upgrade hook
`create-network-class`, `files/network-class-step.py`) re-checks the address
plan, the default CIDRs and the rules, and then:

- fails when the default VirtualNetwork CIDR overlaps `excludedIpv4Cidrs`;

- with `k8sManager: cudn_evpn` and provider networking enabled, fails unless
  `CUDN_EVPN_DNS_RESOLVERS` lists IPv4 addresses, each inside
  `excludedIpv4Cidrs`;
- creates the NetworkClass when none exists;
- otherwise fails with "NetworkClass <field>[, <field>] cannot change after
  installation; see Replacing the NetworkClass" when `fabric_manager`,
  `k8s_manager`, `spec.defaults`, `spec.east_west_capabilities` or the address
  plan in force (`spec.address_plan`) differ. The NetworkClass spec is
  immutable after creation.

## Admission policy

With the fulfillment service enabled, the chart creates the
ValidatingAdmissionPolicy `<namespace>-osac-managed-resources` and its binding.
They reject, from identities other than the OSAC controllers (the `hub-access`
service account the fulfillment controllers use, the OSAC operator and BMF
service accounts, and the Kubernetes garbage collector and namespace
controller), with "OSAC manages this resource; change it through the
fulfillment API":

- create, update and delete of every OSAC networking CR and NetworkAttachment;
- delete of ComputeInstance, BareMetalInstance and ClusterOrder CRs;
- any update that removes an `osac.openshift.io/` finalizer.

Install one OSAC release per cluster: cluster-scoped OSAC resources match every
release's policy. `helm uninstall` removes the policy first;
`scripts/teardown.sh` deletes it before deleting OSAC CRs.

## CI profiles

| Profile | Provider networking | `fabricManager` | `k8sManager` |
|---|---|---|---|
| `vmaas-ci`, `caas-ci`, `full-ci`, `dev` | disabled | `netris` | `cudn_evpn` |
| `bmaas-ci` (and `bmaas-netbox-ci`) | disabled | `netris` | `""` |
| `cudn-evpn-netris-test` | enabled | `netris` | `cudn_evpn` |

With provider networking disabled the profiles need no network manager
registrations or provider settings. Manager registrations are required when
provider networking is enabled.
