# Container `nats.conf` / `resolver_preload` evidence

## Existing-account new-user case
- Container file before new user: `container-before.conf`
- SHA256 before: `11dd3ada44999d25c814d5658b5255a2098da2b5b4891491d509b36328a97646`
- Container file after manual new user add/connect: `container-after-manual-new-user.conf`
- SHA256 after manual new user: `11dd3ada44999d25c814d5658b5255a2098da2b5b4891491d509b36328a97646`
- Conclusion: adding a new User under an existing Account did **not** change container `/etc/nats/nats.conf` or `resolver_preload`.

## New-account case
- Container file after new account preload + reload: `container-after-new-account.conf`
- SHA256 after new account: `cd434181310928245914acf69dfb48e8cde306789a45009062bc4103b2479e0a`
- Observed `resolver_preload`: now contains both SERVICEA and SERVICEB account JWT entries.
- Conclusion: adding a new Account **did** change container `/etc/nats/nats.conf` / `resolver_preload`.
