# Verified single-node results — runtime Account/User addition PoC

This file preserves the verified outcome of the single-node JWT auth PoC executed in `jwtRuntimeAccountUserUpdate/`.

## Scope
- Topology: single NATS node
- Auth model: `operator` + `resolver: MEMORY` + `resolver_preload`
- Server version: NATS 2.12.2
- Question: during runtime, can we add a new User under an existing Account without restart, and can we onboard a new Account without full restart?

## Verified findings
1. A **new User under an already-preloaded Account (`SERVICEA`)** connected successfully **without restart and without reload**.
2. A **new Account (`SERVICEB`) not present in `resolver_preload`** failed with **`nats: Authorization Violation`**.
3. After updating `nats.conf` to add the new account JWT and sending **HUP reload**, the new account's user connected successfully **without full restart**.
4. Reload was verified as **not a restart** because the container `StartedAt` timestamp stayed unchanged and the pre-existing connection remained `CONNECTED -> CONNECTED`.

## Evidence files
- `readme.md`
- `run-output.txt`

## Key lecture takeaway
For this single-node `resolver_preload` setup, the operational distinction is:
- **existing account + new user** → works immediately
- **new account** → requires the server to learn that account JWT; in this PoC, **config update + reload** was sufficient, **full restart was not required**
