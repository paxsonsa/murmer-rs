# Administration & Security

A murmer node is identified by a cryptographic key, not by its address. This page
covers what that means for running a cluster: generating and storing node keys,
using the `murmer` CLI, and operating the zero-trust allowlist that controls which
nodes are allowed to join.

If you have used earlier versions where a node was just a `name@host:port` with a
shared cookie, the short version is: the cookie still exists as a coarse gate, but
the real identity and authorization now come from a per-node key.

## Node identity keys

Every clustered node has a secret key on disk. The public half of that key is the
node's **endpoint id** (an ed25519 public key), and that id is the node's stable
identity. It stays the same across restarts and even if the node's IP address
changes. The `host:port` you configure is now just a hint that helps other nodes
find it.

The key lives in a file. By default that file is `murmer-node.key` in the working
directory. Set an explicit path in config:

```rust,ignore
let config = ClusterConfig::builder()
    .name("alpha")
    .key_path("/etc/murmer/alpha.key")   // persist the identity here
    .listen("0.0.0.0:7100".parse()?)
    .cookie("my-cluster-secret")
    .build()?;
```

On first start the file is created (with `0600` permissions on Unix) and a new key
is generated. On every start after that the existing key is loaded, so the
endpoint id is stable.

Three things to keep in mind:

- **Back the key up.** If the file is lost, the node returns with a different
  endpoint id. Every allowlist entry and seed reference that pointed at the old id
  is now stale, and the node has to be re-admitted.
- **One key per node.** Because the default path is relative to the working
  directory, two nodes started in the same directory load the same key and end up
  with the same identity, which breaks the cluster. Give each node its own
  `key_path`.
- **The key is sensitive.** Anyone with the key file can impersonate that node.
  Treat it like an SSH private key.

## The `murmer` CLI

The `murmer` binary manages keys and the allowlist. It works on files and does not
talk to a running node, so you can use it during deployment or from a shell. Build
it with the `cli` feature:

```sh
cargo run -p murmer --features cli --bin murmer -- <command>
# or install it: cargo install --path murmer --features cli
```

### Print a node's endpoint id

```sh
murmer id --key /etc/murmer/alpha.key
# 5e9c2da5...f0a8
```

This loads the key file (creating it if it does not exist) and prints the endpoint
id. This is how you find the id to put in another node's allowlist, or to hand to a
joiner as a seed.

### Manage the allowlist

```sh
murmer allow add  <endpoint-id> --file /etc/murmer/allow.txt
murmer allow rm   <endpoint-id> --file /etc/murmer/allow.txt
murmer allow list               --file /etc/murmer/allow.txt
```

The allowlist file is plain text, one endpoint id per line, with `#` comments. You
can edit it by hand or with the CLI.

## The allowlist

The allowlist is murmer's zero-trust authorization layer. There are two gates a
peer has to pass to join a cluster:

1. **The cookie**, a shared secret checked during the handshake. This is a coarse
   "are you even talking to this cluster" check. It has not changed.
2. **The allowlist**, a set of endpoint ids. This is the real authorization. A node
   admits a peer only if that peer's endpoint id is on its list.

Because iroh authenticates each connection against the peer's key, a peer cannot
lie about its identity to get past the allowlist. The cookie alone is no longer
enough to become a member.

The allowlist has two modes:

- **`Open`** (the default). Any peer with the correct cookie is admitted. This
  matches the old cookie-only behavior, and is convenient for local development.
- **`Enforced(path)`**. Only peers whose endpoint id appears in the file at `path`
  are admitted.

```rust,ignore
let config = ClusterConfig::builder()
    .name("alpha")
    .key_path("/etc/murmer/alpha.key")
    .listen("0.0.0.0:7100".parse()?)
    .cookie("my-cluster-secret")
    .allowlist("/etc/murmer/allow.txt")   // switch to Enforced mode
    .build()?;
```

Enforcement runs in both directions. A node checks the allowlist when it accepts an
inbound connection and before it dials out to a peer it learned about through
discovery or gossip. Gossip carries addressing hints only. A node never treats a
gossiped peer as authorized just because another node mentioned it. The rule is
simple: a node is a member if, and only if, it is on the local allowlist.

### Hot reload and revocation

The allowlist file is watched. When you change it, the running node picks up the
change within about a second, with no restart:

- **Adding** an id lets a previously rejected peer connect.
- **Removing** an id drops any live connection to that peer right away and refuses
  to reconnect. In-flight requests to a revoked peer fail fast.

This means you manage membership by editing files (by hand, with the CLI, or
through your config management tooling), and the cluster reacts on its own.

## Operational workflows

### Adding a node to a running cluster

Adding a node X means every existing node has to trust X's key, and X has to trust
theirs. Trust is mutual.

1. On X, get its endpoint id: `murmer id --key /etc/murmer/x.key`.
2. Add X's id to every existing node's allowlist file (or to a single shared file
   on a mount). Use `murmer allow add <x-id> --file ...` or your config management.
3. Make sure X's own allowlist contains the nodes it should reach.
4. Start X with a seed pointing at one of the existing nodes.

No restarts are needed. The existing nodes pick up X's id within a second and admit
it when it connects.

### Removing or revoking a node

Remove the node's endpoint id from the allowlist files. Every node that drops the id
closes its live connection to that node within a second and refuses to let it back
in. This is how you evict a compromised or decommissioned node.

### Distributing the allowlist

murmer does not gossip the allowlist between nodes. Distributing the file is your
job, which usually means it is already solved by whatever you use to ship config:
Ansible, a Kubernetes ConfigMap, a shared mount, or a baked image. Point every node
at the same logical file, and updates roll out the way the rest of your config does.

## What the cookie is still for

The cookie has not gone away. It is a cheap first check that keeps unrelated
clusters and obvious noise from getting as far as the key-level authorization. Set
it to a real secret. Treat the allowlist as the layer that decides membership, and
the cookie as the layer that scopes which cluster a node is trying to reach.

## Edge clients

Edge clients (see [Edge Clients](./edge-clients.md)) generate a fresh ephemeral key
on each run, since they only dial out and never accept connections. If the server
runs with an enforced allowlist, that ephemeral key is not admitted. Either point
edge clients at nodes running in `Open` mode, or give the client a persistent key
and add it to the server's allowlist like any other peer.
