# CUDN EVPN/Netris VMaaS + BMaaS E2E profile

This is the explicit opt-in installer profile for the Phase 1 IPv4 CUDN EVPN
environment used with VMaaS and BMaaS, with provider networking enabled. VMs
and bare-metal instances use the same NetworkClass and can therefore be
attached to the same VirtualNetwork. It is intentionally separate from the
base chart and all normal profiles.

The profile provides:

- VMaaS and BMaaS (Metal3 backend) with `global.networking.provisioningEnabled: true`;
- `fabricManager: netris` and `k8sManager: cudn_evpn`; the installer's
  NetworkClass step creates the NetworkClass from them;
- `fabric-registration.yaml`, the Netris Fabric Manager registration
  (`network_types: evpn`), which `make install-osac` applies after the chart.
  The installer registers the OSAC `cudn_evpn` Kubernetes Manager itself;
- the non-secret Netris settings in `aap.instanceGroups.networkFulfillment.config`.

Before installing, the target OpenShift cluster must already have the Phase 1
EVPN/BGP/VTEP prerequisites, the EVPN `FRRConfiguration`, and the Metal3
BareMetalHost inventory needed by BMaaS. This profile does not install the FRR
operator, create the `FRRConfiguration`, or provision the external fabric.

Site-specific settings and the Netris password must be supplied through a
private values file, for example `cudn-evpn-netris-test-secrets.local.yaml`:

```yaml
global:
  networking:
    excludedIpv4Cidrs:
    - 192.0.2.0/24        # management, provisioning and node networks
    - 198.51.100.53/32    # DNS resolvers given to workloads
aap:
  instanceGroups:
    networkFulfillment:
      config:
        CUDN_EVPN_DNS_RESOLVERS: "198.51.100.53"
        NETRIS_DNS_RESOLVERS: "198.51.100.53"
        NETRIS_DNS_SNAT_CIDR: "192.0.2.128/26"
        NETRIS_VNI_RANGE: "100000-100999"
        NETRIS_HUB_AS: "65000"
        NETRIS_DHCP_OPTION_SET_ID: "1"
        NETRIS_PROVISIONING_VNET: "provisioning"
      secret:
        NETRIS_PASSWORD: "<password>"
```

The NetworkClass step fails the rollout unless every `CUDN_EVPN_DNS_RESOLVERS`
address is inside `excludedIpv4Cidrs`. From `osac-installer/`:

```bash
make install PLATFORM=openshift PROFILE=cudn-evpn-netris-test NS=osac \
  INSTANCE_VALUES_EXTRA="-f cudn-evpn-netris-test-secrets.local.yaml"
```

The Makefile profile selection is the installation wiring for this profile;
the standard CI workflows continue to use their existing profiles.
