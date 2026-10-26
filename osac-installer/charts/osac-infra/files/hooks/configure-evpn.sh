#!/usr/bin/env bash

set -euo pipefail

CONFIG_DIR="${CONFIG_DIR:-/config}"
# shellcheck disable=SC1091
source "${CONFIG_DIR}/settings.env"

wait_until() {
    local timeout_seconds="$1"
    local description="$2"
    shift 2

    local started_at="$SECONDS"
    while true; do
        if "$@" >/dev/null 2>&1; then
            return 0
        fi
        if (( SECONDS - started_at >= timeout_seconds )); then
            echo "Timed out waiting for ${description}" >&2
            return 1
        fi
        sleep 10
    done
}

network_configuration_ready() {
    local providers route_advertisements routing_via_host ip_forwarding
    providers="$(oc get network.operator.openshift.io cluster \
        -o jsonpath='{range .spec.additionalRoutingCapabilities.providers[*]}{@}{","}{end}')"
    route_advertisements="$(oc get network.operator.openshift.io cluster \
        -o jsonpath='{.spec.defaultNetwork.ovnKubernetesConfig.routeAdvertisements}')"
    routing_via_host="$(oc get network.operator.openshift.io cluster \
        -o jsonpath='{.spec.defaultNetwork.ovnKubernetesConfig.gatewayConfig.routingViaHost}')"
    ip_forwarding="$(oc get network.operator.openshift.io cluster \
        -o jsonpath='{.spec.defaultNetwork.ovnKubernetesConfig.gatewayConfig.ipForwarding}')"
    [[ "${providers}" == "FRR," \
        && "${route_advertisements}" == "Enabled" \
        && "${routing_via_host}" == "true" \
        && "${ip_forwarding}" == "Global" ]]
}

network_operator_ready() {
    local available progressing degraded
    available="$(oc get clusteroperator network \
        -o jsonpath='{.status.conditions[?(@.type=="Available")].status}')"
    progressing="$(oc get clusteroperator network \
        -o jsonpath='{.status.conditions[?(@.type=="Progressing")].status}')"
    degraded="$(oc get clusteroperator network \
        -o jsonpath='{.status.conditions[?(@.type=="Degraded")].status}')"
    [[ "${available}" == "True" \
        && "${progressing}" == "False" \
        && "${degraded}" == "False" ]]
}

frr_pod_ready() {
    local pods name ready
    pods="$(oc get pods -n openshift-frr-k8s \
        -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.status.conditions[?(@.type=="Ready")].status}{"\n"}{end}')"
    while read -r name ready; do
        if [[ "${name}" == frr-k8s-* && "${ready}" == "True" ]]; then
            return 0
        fi
    done <<< "${pods}"
    return 1
}

nmstate_csv_ready() {
    local csvs name phase
    csvs="$(oc get csv -n openshift-nmstate \
        -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.status.phase}{"\n"}{end}')"
    while read -r name phase; do
        if [[ "${name}" == kubernetes-nmstate-operator* \
            && "${phase}" == "Succeeded" ]]; then
            return 0
        fi
    done <<< "${csvs}"
    return 1
}

vtep_allocated() {
    local accepted reason
    accepted="$(oc get vtep.k8s.ovn.org tenant-vtep \
        -o jsonpath='{.status.conditions[?(@.type=="Accepted")].status}')"
    reason="$(oc get vtep.k8s.ovn.org tenant-vtep \
        -o jsonpath='{.status.conditions[?(@.type=="Accepted")].reason}')"
    [[ "${accepted}" == "True" && "${reason}" == "Allocated" ]]
}

bgp_session_established() {
    local node="$1"
    local peer="$2"
    local sessions status
    sessions="$(oc get bgpsessionstates.frrk8s.metallb.io \
        -n openshift-frr-k8s \
        -l "frrk8s.metallb.io/peer=${peer},frrk8s.metallb.io/node=${node}" \
        -o jsonpath='{range .items[*]}{.status.bgpStatus}{"\n"}{end}')"
    while read -r status; do
        if [[ "${status}" == "Established" ]]; then
            return 0
        fi
    done <<< "${sessions}"
    return 1
}

echo "Checking EVPN cluster prerequisites"
network_type="$(oc get network.operator.openshift.io cluster \
    -o jsonpath='{.spec.defaultNetwork.type}' 2>/dev/null || true)"
if [[ "${network_type}" != "OVNKubernetes" ]]; then
    echo "ERROR: EVPN requires defaultNetwork.type OVNKubernetes; found '${network_type}'" >&2
    echo "No cluster configuration was changed" >&2
    exit 1
fi

while IFS= read -r node_name || [[ -n "${node_name:-}" ]]; do
    if [[ -z "${node_name}" ]]; then
        continue
    fi
    if ! oc get node "${node_name}" >/dev/null 2>&1; then
        echo "ERROR: configured EVPN node '${node_name}' does not exist" >&2
        echo "No cluster configuration was changed" >&2
        exit 1
    fi
done < "${CONFIG_DIR}/nodes.txt"

if [[ "${CONFIGURE_CLUSTER_NETWORK}" == "true" ]]; then
    echo "Patching cluster network operator for FRR and route advertisements"
    oc patch network.operator.openshift.io cluster \
        --type=merge --patch-file "${CONFIG_DIR}/cluster-network-patch.json"
    wait_until 300 "cluster network operator EVPN settings" network_configuration_ready
    sleep 30
    wait_until 1800 "network cluster operator to finish its rollout" network_operator_ready
fi

for crd in \
    frrconfigurations.frrk8s.metallb.io \
    bgpsessionstates.frrk8s.metallb.io \
    vteps.k8s.ovn.org \
    routeadvertisements.k8s.ovn.org; do
    wait_until 900 "CRD ${crd} to be created" oc get crd "${crd}"
    oc wait --for=condition=Established "crd/${crd}" --timeout=300s
done
wait_until 900 "an FRR-K8s pod to become Ready" frr_pod_ready

echo "Waiting for NMState operator"
wait_until 900 "NMState operator CSV to succeed" nmstate_csv_ready
oc apply -f "${CONFIG_DIR}/nmstate.yaml"
oc wait nmstate/nmstate --for=condition=Available --timeout=900s
wait_until 900 "NodeNetworkConfigurationPolicy CRD to be created" \
    oc get crd nodenetworkconfigurationpolicies.nmstate.io
oc wait --for=condition=Established \
    crd/nodenetworkconfigurationpolicies.nmstate.io --timeout=300s

echo "Applying per-node EVPN network policies"
oc apply -f "${CONFIG_DIR}/nncp.yaml"
while IFS= read -r node_name || [[ -n "${node_name:-}" ]]; do
    [[ -z "${node_name}" ]] && continue
    if ! oc wait "nncp/osac-evpn-${node_name}" \
        --for=condition=Available --timeout=900s; then
        echo "NNCP readiness failed; current NNCP and NNCE state:" >&2
        oc get nncp,nnce || true
        exit 1
    fi
done < "${CONFIG_DIR}/nodes.txt"

echo "Applying tenant VTEP"
oc apply -f "${CONFIG_DIR}/vtep.yaml"
wait_until 900 "VTEP tenant-vtep to be accepted and allocated" vtep_allocated

echo "Applying underlay FRR configuration and route advertisements"
oc apply -f "${CONFIG_DIR}/frr.yaml"
oc apply -f "${CONFIG_DIR}/routeadvertisements.yaml"

while IFS= read -r resource; do
    [[ -z "${resource}" ]] && continue
    resource_name="${resource##*/}"
    desired=false
    while IFS= read -r node_name || [[ -n "${node_name:-}" ]]; do
        if [[ "${resource_name}" == "osac-evpn-${node_name}" ]]; then
            desired=true
            break
        fi
    done < "${CONFIG_DIR}/nodes.txt"
    if [[ "${desired}" == "false" ]]; then
        oc delete "${resource}"
        echo "Deleted stale NNCP ${resource_name}; deleting it does not remove interface addresses from the node."
    fi
done < <(oc get nncp -l osac.openshift.io/evpn=true -o name)

while IFS= read -r resource; do
    [[ -z "${resource}" ]] && continue
    resource_name="${resource##*/}"
    desired=false
    while IFS= read -r node_name || [[ -n "${node_name:-}" ]]; do
        if [[ "${resource_name}" == "osac-evpn-${node_name}" \
            || "${resource_name}" == "osac-evpn-raw-${node_name}" ]]; then
            desired=true
            break
        fi
    done < "${CONFIG_DIR}/nodes.txt"
    if [[ "${desired}" == "false" ]]; then
        oc delete -n openshift-frr-k8s "${resource}"
        echo "Deleted stale FRRConfiguration ${resource_name}."
    fi
done < <(oc get frrconfigurations -n openshift-frr-k8s \
    -l osac.openshift.io/evpn=true -o name)

if [[ "${WAIT_FOR_ESTABLISHED}" == "true" ]]; then
    while read -r node_name peer_address \
        || [[ -n "${node_name:-}${peer_address:-}" ]]; do
        [[ -z "${node_name:-}" || -z "${peer_address:-}" ]] && continue
        if ! wait_until "${BGP_TIMEOUT_SECONDS}" \
            "BGP session from ${node_name} to ${peer_address}" \
            bgp_session_established "${node_name}" "${peer_address}"; then
            oc get bgpsessionstates.frrk8s.metallb.io -A -o wide || true
            echo "node ${node_name}: BGP session to ${peer_address} not Established; check the fabric side and the evpn values" >&2
            exit 1
        fi
    done < "${CONFIG_DIR}/peers.txt"
fi

echo "EVPN host networking is configured. RouteAdvertisements may show ConfigurationPending until OSAC creates the first CUDN; that is expected."
