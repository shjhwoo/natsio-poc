# Verified 3-node cluster results — runtime User permission mutation PoC

## Outcome
- Direct connections to node1/node2/node3 all showed the same baseline behavior with v1 creds: `servicea.base` allowed, `servicea.extra` denied.
- After generating v2 creds, **new connections to all three nodes** immediately allowed `servicea.extra` without server restart or reload.
- After generating v3 creds, **new connections to all three nodes** immediately denied `servicea.base` and allowed `servicea.extra` without server restart or reload.
- A representative existing v2 connection kept its previous permissions after v3 creds were generated.

## Conclusion
In this 3-node cluster where all nodes already trusted the same account, **user-permission changes propagated to new connections on every node without restart/reload**, while **existing sessions were not automatically re-authorized**.
