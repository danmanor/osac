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

With `evpn.enabled: true` in an infra values file, the installer configures the
OpenShift side of EVPN: FRR, route advertisements, NMState, per-node uplink and
VTEP addresses, the `tenant-vtep`, and fabric BGP sessions. The Netris fabric
side remains external; the fabric admin configures the links, VTEP range, and
switch-side node ASN. The `cudn_evpn` manager creates the CUDN later during
OSAC operation.
The Metal3 BareMetalHost inventory needed by BMaaS remains a separate
prerequisite.

Supply Netris credentials and networking settings through a private instance
values file, for example `cudn-evpn-netris-test-secrets.local.yaml`:

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
address is inside `excludedIpv4Cidrs`.

Supply site-specific EVPN addresses through a private infra values file. For
example, save this as `cudn-evpn-netris-test-evpn.private.yaml` and replace
the sample node, interfaces, addresses, and ASNs with values from the cluster
and fabric admins:

```yaml
evpn:
  enabled: true
  nmstate:
    install: true
    channel: stable
  configureClusterNetwork: true
  vtepCIDR: 10.200.255.0/24
  bgp:
    asn: 4209999999
    acceptPrefixes:
      - 10.200.0.0/16
    waitForEstablished: true
    timeoutSeconds: 900
  nodes:
    - name: worker-0
      vtepIP: 10.200.255.1
      uplinks:
        - interface: ens6
          address: 169.254.133.2/31
          peerAddress: 169.254.133.3
          peerASN: 4200300002
```

From `osac-installer/`:

```bash
make install PLATFORM=openshift PROFILE=cudn-evpn-netris-test NS=osac \
  INFRA_VALUES_EXTRA="-f cudn-evpn-netris-test-evpn.private.yaml" \
  INSTANCE_VALUES_EXTRA="-f cudn-evpn-netris-test-secrets.local.yaml"
```

The Makefile profile selection is the installation wiring for this profile;
the standard CI workflows continue to use their existing profiles.
