# Verified single-node results — runtime User permission mutation PoC

## Outcome
- Existing `SERVICEA/servicea-user` connection created with v1 creds (`servicea.base` only) kept denying `servicea.extra` even after v2 creds were generated.
- A new v2 connection immediately allowed both publish and subscribe on `servicea.extra` without server restart or reload.
- After generating v3 creds (`servicea.extra` only), the existing v2 connection still allowed `servicea.base`.
- A new v3 connection immediately denied `servicea.base` and allowed `servicea.extra` without server restart or reload.

## Conclusion
For a user under an already-known account, **permission changes in the user JWT were reflected on new connections without restart/reload**, but **already-connected sessions kept their earlier permission set**.
