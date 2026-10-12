# OSAC networking contract in brief

This is a reading guide to the proposed networking contract. The implementation
is still catching up with the designs. See the [provider guide](network-manager-provider-guide.md)
for packaging, payloads, task names, and implementation requirements.

## The agreement

**OSAC owns the workflow. The provider owns the backend behavior.**

| Piece | Responsibility |
|---|---|
| OSAC | Validate tenant requests, choose the registered manager, provide resolved inputs, track jobs, store results, and order cleanup. |
| Fabric manager | Tenant networks, physical interfaces and policy, external addresses, NAT, and CaaS ingress reservations. |
| Kubernetes manager | VM overlay networking and VM interface policy. Needed for VM support. |
| Workload controllers | Create VMs, provision servers and clusters, and wait at networking gates before starting traffic or reusing resources. |

A provider supplies Ansible entry points for every task assigned to its role.
Those tasks can call any backend API or controller. The provider installs the
adapters and their dependencies in the execution environment, registers the
manager and its data models, supplies configuration/secrets, and selects the
manager through the deployment's NetworkClass.

No OSAC provider-specific source change should be needed once the generic
contract and installer work are implemented. Infrastructure prerequisites and
actual network behavior remain the provider's responsibility.

Here, “networking profile” means the selected NetworkClass and managers, not
another resource. NetworkClass is OSAC configuration, not a tenant network.
It has no provider create/delete task or runtime outputs. Shared provider
settings use the existing installation/environment configuration.

Separate VirtualNetworks may reuse private CIDRs and IPs. Two independent
endpoints sharing one Subnet must use different IPs. Address allocation and
cleanup always retain the owning network identity.

## The three data objects

| Object | Example |
|---|---|
| `NetworkDataModel` | “A Subnet VLAN ID is an integer from 1 to 4094.” The definition includes its meaning and owner scope. |
| `NetworkManager` | “My Fabric manager produces that model”; “my Kubernetes manager requires it.” |
| `NetworkData` | “This Subnet's VLAN ID is 217.” The immutable value for one API resource, stored authoritatively by fulfillment-service and projected as a CR in the networking hub. |

A Fabric create or reconcile task returns missing values for declared owners
present in its `network_owner_context`, including NetworkClass, VirtualNetwork,
and Subnet owners. A NetworkClass-scoped value can first be returned by the
first eligible resource operation; a Subnet operation can first return a
VirtualNetwork-scoped value. OSAC validates and stores the complete result
before starting consumers, and identical existing writes succeed. The
fulfillment service is authoritative and projects each record as a
NetworkData CR in the networking hub. OSAC reads values through its private
API and supplies them in AAP inputs; provider roles need neither database
credentials nor direct CR access.
Concurrent tasks that can produce the same missing owner/model value must
converge on one backend allocation and return the same value.
Matching model names proves declared compatibility; it does not prove that
packets can flow or that policy is enforced.

## One normal flow

```mermaid
sequenceDiagram
    participant OSAC
    participant Fabric
    participant K8s as Kubernetes manager
    participant Workload
    OSAC->>Fabric: Create network and Subnet
    Fabric-->>OSAC: Required network values
    OSAC->>OSAC: Validate and store values
    OSAC->>K8s: Create VM overlay using stored values
    K8s-->>OSAC: Success
    Workload->>Workload: Prepare stopped VM and stable identity
    Workload->>OSAC: SubnetAttachment request
    OSAC->>K8s: Apply VM interface and policy
    K8s-->>OSAC: Success for the request UID
    OSAC-->>Workload: Attachment ready; VM may start
```

The internal `SubnetAttachment` tracks networking work. Its target is a VM or
a BaremetalInstance; it is not itself the workload. The manager receives both
the request identity and the target identity, plus one resolved interface,
Subnet and complete SecurityGroup rules.

BMaaS uses Fabric for that attachment. BMF waits for apply, performs the
handoff reboot, then networking discovers the lease using the selected port's
MAC. CaaS creates one such request for each bare-metal worker. OSAC verifies
the worker's parent Cluster and supplies that Cluster's tenant as the network
context, even though the hidden worker itself belongs to `system`.

For VMs, OSAC chooses the namespace and creates the stopped VM; the Kubernetes
Manager configures its interface and policy through the existing attachment
task. OSAC starts the VM after success and preserves the manager's fields.
Providers do not need a new task or output channel to choose their binding.

## How CaaS gets its ingress address

Fabric reserves an IP and returns it. OSAC records that IP in the ClusterOrder
annotation `osac.openshift.io/ingress-allocated-address`, then configures
MetalLB with it. Ingress becomes ready only after the Service confirms that
address. Fabric handles uniqueness and retry recovery; OSAC has no second
IPAM registry. Normal cleanup removes consumers, asks Fabric to release the
address, and then removes the annotation.

## Which identity owns the network?

The stable OSAC API resource ID owns backend allocations. Kubernetes UIDs
protect writes to particular objects. If a hub projection is recreated, OSAC
recovers its existing jobs and allocations under the same API ID. Deleting an
API resource and creating another gives a new ID and a new owner.

## What happens on failures?

- OSAC records the operation before launching it. A lost acknowledgment stays
  unresolved until OSAC can account for the original job; it does not start
  a conflicting delete or blindly launch again.
- Job success is followed by result validation and durable persistence. A
  restart resumes unfinished persistence using the same accepted result.
- Every AAP job is idempotent: rerunning after success or interruption converges
  without duplicate effects. Providers reuse owned resources, recover partial
  work, and support safe reruns after compatible adapter upgrades.
- Normal deletion removes consumers before producers and retains finalizers
  until cleanup is confirmed. Manager replacement waits for every bound
  resource and operation to drain.
- Globally disabled provisioning has a separate documented skip behavior;
  it does not claim the backend was cleaned up. Re-enabling requires draining
  existing workloads and reconciling provider leftovers. OSAC blocks new
  workload creation until the transition is ready; it does not migrate running
  workloads automatically.

## Can a provider use a no-op?

Yes. Its Ansible task may do nothing and return success. The entry point must
still exist, return the standard envelope, and supply required real values.
An address-allocation task cannot invent an IP that was never reserved.

OSAC trusts the provider's report. A no-op policy task is not proof that
SecurityGroups are enforced. Providers document their no-ops and own the
consequences; behavioral checks establish which outcomes they actually deliver.

## What must implementation tasks cover?

The generic registry and installer, durable operation tracking, validated
value exchange, workload gates, CaaS address ownership, provider adapters,
and a provider-runnable selection of OSAC's own E2E tests all need explicit coverage. Completing
registration alone does not complete bring-your-own networking.
