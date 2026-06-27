# Container `nats.conf` / `resolver_preload` evidence for user permission mutation

## Single node
- before SHA256: `65959f3ae9feac9ef34c82ac837d147815ced44855528026aa9e90e36a28be15`
- after SHA256: `65959f3ae9feac9ef34c82ac837d147815ced44855528026aa9e90e36a28be15`
- Conclusion: changing User pub/sub permissions did **not** change container `/etc/nats/nats.conf` / `resolver_preload`.

## 3-node cluster
- n1 before/after: `9714909ec222616e553e3f4b1f87d66286de45e9281f4deb1ee9499022c9b9e2` / `9714909ec222616e553e3f4b1f87d66286de45e9281f4deb1ee9499022c9b9e2`
- n2 before/after: `0b72d88a2240ab58a5e8df63b9853b52e1ced5c3c786a4b996c0b94f482c3005` / `0b72d88a2240ab58a5e8df63b9853b52e1ced5c3c786a4b996c0b94f482c3005`
- n3 before/after: `47ed16eec57089b8757dee13076f0164875d203ef9ee2e27407d574ad2e61b3d` / `47ed16eec57089b8757dee13076f0164875d203ef9ee2e27407d574ad2e61b3d`
- Conclusion: changing User pub/sub permissions did **not** change container `/etc/nats/nats.conf` / `resolver_preload` on any node.

## Interpretation
`resolver_preload` stores account JWT preload entries. User creation and user-permission edits happen at the user JWT/creds layer, so in this setup they did not mutate container `nats.conf`.
