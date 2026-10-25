# noop_network

The reference no-op network manager of the network manager contract test plan.
Every task of the Fabric task set is a documented no-op: it changes nothing and
publishes no `osac_result`. OSAC uses it in the osac-aap integration tests of
the generic playbook (`tests/integration/targets/network_task_playbook`); it is
never a production manager.

Registering it with the operator works only for tasks that need no output: the
operator rejects a `create_external_ip`, `reserve_cluster_ingress_ip` or
`query_dhcp_lease` result without `external_ip`, `cluster_ingress` or
`dhcp_lease`, so those tasks fail as final with this manager.
