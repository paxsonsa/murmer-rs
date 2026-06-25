//! Zero-trust peer allowlist.
//!
//! The allowlist is the real authorization gate of the cluster: a peer is a
//! member **iff its iroh [`EndpointId`] is in the local allowlist**. The cookie
//! handshake is only a coarse "are you dialing this cluster at all" check;
//! authorization is by cryptographic key.
//!
//! Enforcement happens in two complementary places:
//! 1. [`AllowlistHook`] — installed on the iroh endpoint via `Builder::hooks`.
//!    Its `after_handshake` fires for **both** inbound and outbound connections
//!    once the remote's `EndpointId` is cryptographically known, and rejects any
//!    key not on the list. This covers the dial side and the accept side at once.
//! 2. Revocation — the file is hot-reloaded; when a key is *removed*, its id is
//!    broadcast on a channel so the transport can drop the live connection.
//!
//! The file is plain text: one z-base-32 `EndpointId` per line, `#` comments and
//! blank lines ignored.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use iroh::EndpointId;
use iroh::endpoint::{AfterHandshakeOutcome, Connection, EndpointHooks, VarInt};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use super::config::AllowlistMode;
use super::error::ClusterError;

/// Close code used when rejecting a connection from a non-allowlisted peer.
const NOT_ALLOWLISTED_CODE: u32 = 3;

/// The zero-trust allowlist. Cheap to clone (`Arc` inside).
#[derive(Clone)]
pub struct Allowlist {
    inner: Arc<Inner>,
}

struct Inner {
    mode: Mode,
}

enum Mode {
    /// Accept any peer (authorization delegated to the cookie alone).
    Open,
    /// Only accept peers whose `EndpointId` is in `set`.
    Enforced {
        set: RwLock<HashSet<EndpointId>>,
        /// Broadcast of ids removed by a hot-reload — subscribers revoke them.
        revoked_tx: broadcast::Sender<EndpointId>,
    },
}

impl Allowlist {
    /// Build an allowlist from config. In `Enforced` mode the file is loaded now
    /// and a background watcher hot-reloads it; removed keys are broadcast for
    /// revocation. The watcher stops when `shutdown` is cancelled.
    pub fn new(mode: AllowlistMode, shutdown: CancellationToken) -> Result<Self, ClusterError> {
        match mode {
            AllowlistMode::Open => Ok(Self {
                inner: Arc::new(Inner { mode: Mode::Open }),
            }),
            AllowlistMode::Enforced(path) => {
                let initial = load_file(&path)?;
                tracing::info!(
                    path = %path.display(),
                    count = initial.len(),
                    "loaded zero-trust allowlist"
                );
                let (revoked_tx, _) = broadcast::channel(64);
                let allow = Self {
                    inner: Arc::new(Inner {
                        mode: Mode::Enforced {
                            set: RwLock::new(initial),
                            revoked_tx: revoked_tx.clone(),
                        },
                    }),
                };
                spawn_watcher(allow.clone(), path, shutdown);
                Ok(allow)
            }
        }
    }

    /// An always-open allowlist (no file, no watcher). Useful for tests.
    pub fn open() -> Self {
        Self {
            inner: Arc::new(Inner { mode: Mode::Open }),
        }
    }

    /// Whether `id` is authorized to participate in the cluster.
    pub fn is_allowed(&self, id: &EndpointId) -> bool {
        match &self.inner.mode {
            Mode::Open => true,
            Mode::Enforced { set, .. } => set.read().unwrap().contains(id),
        }
    }

    /// Whether this allowlist actively enforces (vs. open).
    pub fn is_enforced(&self) -> bool {
        matches!(self.inner.mode, Mode::Enforced { .. })
    }

    /// Subscribe to revocation events (ids removed from the allowlist on reload).
    /// Returns `None` in open mode (nothing is ever revoked).
    pub fn subscribe_revocations(&self) -> Option<broadcast::Receiver<EndpointId>> {
        match &self.inner.mode {
            Mode::Open => None,
            Mode::Enforced { revoked_tx, .. } => Some(revoked_tx.subscribe()),
        }
    }

    /// Replace the in-memory set, broadcasting any removed ids for revocation.
    fn apply_reload(&self, next: HashSet<EndpointId>) {
        let Mode::Enforced { set, revoked_tx } = &self.inner.mode else {
            return;
        };
        let removed: Vec<EndpointId> = {
            let mut guard = set.write().unwrap();
            let removed = guard.difference(&next).copied().collect::<Vec<_>>();
            *guard = next;
            removed
        };
        for id in removed {
            tracing::warn!(endpoint_id = %id, "allowlist revoked peer — dropping connection");
            let _ = revoked_tx.send(id);
        }
    }

    /// Build the iroh endpoint hook that enforces this allowlist on every
    /// connection (inbound and outbound) after the TLS handshake.
    pub fn hook(&self) -> AllowlistHook {
        AllowlistHook {
            allowlist: self.clone(),
        }
    }
}

/// iroh [`EndpointHooks`] implementation enforcing the allowlist.
#[derive(Debug)]
pub struct AllowlistHook {
    allowlist: Allowlist,
}

impl std::fmt::Debug for Allowlist {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Allowlist")
            .field("enforced", &self.is_enforced())
            .finish()
    }
}

impl EndpointHooks for AllowlistHook {
    // Mirror the trait's `-> impl Future + Send` shape rather than `async fn` so
    // the `Send` bound the hook machinery requires stays explicit.
    #[allow(clippy::manual_async_fn)]
    fn after_handshake<'a>(
        &'a self,
        conn: &'a Connection,
    ) -> impl Future<Output = AfterHandshakeOutcome> + Send + 'a {
        async move {
            let id = conn.remote_id();
            if self.allowlist.is_allowed(&id) {
                AfterHandshakeOutcome::Accept
            } else {
                tracing::warn!(endpoint_id = %id, "rejecting connection: not in allowlist");
                AfterHandshakeOutcome::Reject {
                    error_code: VarInt::from_u32(NOT_ALLOWLISTED_CODE),
                    reason: b"endpoint not in allowlist".to_vec(),
                }
            }
        }
    }
}

// =============================================================================
// FILE I/O — plain text, one z-base-32 EndpointId per line
// =============================================================================

/// Parse an allowlist file into a set of endpoint ids. Missing file = empty set
/// (a node that trusts nobody yet, rather than a hard error).
pub fn load_file(path: impl AsRef<Path>) -> Result<HashSet<EndpointId>, ClusterError> {
    let path = path.as_ref();
    let text = match std::fs::read_to_string(path) {
        Ok(t) => t,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(HashSet::new()),
        Err(e) => {
            return Err(ClusterError::AllowlistFile(format!(
                "read {}: {e}",
                path.display()
            )));
        }
    };
    let mut set = HashSet::new();
    for (lineno, raw) in text.lines().enumerate() {
        let line = raw.split('#').next().unwrap_or("").trim();
        if line.is_empty() {
            continue;
        }
        let id: EndpointId = line.parse().map_err(|e| {
            ClusterError::AllowlistFile(format!(
                "{}:{}: invalid endpoint id {line:?}: {e}",
                path.display(),
                lineno + 1
            ))
        })?;
        set.insert(id);
    }
    Ok(set)
}

/// Atomically write an allowlist file (one id per line, sorted for stable diffs).
pub fn write_file(path: impl AsRef<Path>, ids: &HashSet<EndpointId>) -> Result<(), ClusterError> {
    let path = path.as_ref();
    let mut lines: Vec<String> = ids.iter().map(|id| id.to_string()).collect();
    lines.sort();
    let body = format!(
        "# murmer cluster allowlist — one endpoint id per line\n{}\n",
        lines.join("\n")
    );
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, body)
        .map_err(|e| ClusterError::AllowlistFile(format!("write {}: {e}", tmp.display())))?;
    std::fs::rename(&tmp, path)
        .map_err(|e| ClusterError::AllowlistFile(format!("rename into {}: {e}", path.display())))?;
    Ok(())
}

/// Add an id to the allowlist file (creating it if needed). Returns true if newly added.
pub fn add_to_file(path: impl AsRef<Path>, id: EndpointId) -> Result<bool, ClusterError> {
    let path = path.as_ref();
    let mut set = load_file(path)?;
    let added = set.insert(id);
    if added {
        write_file(path, &set)?;
    }
    Ok(added)
}

/// Remove an id from the allowlist file. Returns true if it was present.
pub fn remove_from_file(path: impl AsRef<Path>, id: &EndpointId) -> Result<bool, ClusterError> {
    let path = path.as_ref();
    let mut set = load_file(path)?;
    let removed = set.remove(id);
    if removed {
        write_file(path, &set)?;
    }
    Ok(removed)
}

// =============================================================================
// HOT-RELOAD WATCHER — poll the file mtime once a second
// =============================================================================

fn spawn_watcher(allowlist: Allowlist, path: PathBuf, shutdown: CancellationToken) {
    tokio::spawn(async move {
        let mut last_fingerprint = file_fingerprint(&path);
        let mut ticker = tokio::time::interval(Duration::from_secs(1));
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let fingerprint = file_fingerprint(&path);
                    if fingerprint != last_fingerprint {
                        last_fingerprint = fingerprint;
                        match load_file(&path) {
                            Ok(next) => {
                                tracing::info!(path = %path.display(), count = next.len(), "allowlist reloaded");
                                allowlist.apply_reload(next);
                            }
                            Err(e) => tracing::error!("allowlist reload failed: {e}"),
                        }
                    }
                }
                _ = shutdown.cancelled() => break,
            }
        }
    });
}

/// Change-detection fingerprint for the allowlist file: a hash of its bytes.
///
/// Hashing the contents — rather than comparing mtime — means an edit that
/// leaves mtime unchanged is still detected: coarse (1s) mtime filesystems, two
/// edits within the same second, or timestamp-preserving tools (`rsync
/// --times`, some editors). An mtime- or size-only check could miss a key
/// removal and leave a revoked peer's connection open. Returns `None` if the
/// file can't be read (treated as "no fingerprint", same as before).
fn file_fingerprint(path: &Path) -> Option<u64> {
    use std::hash::{Hash, Hasher};
    let bytes = std::fs::read(path).ok()?;
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    bytes.hash(&mut hasher);
    Some(hasher.finish())
}

#[cfg(test)]
mod tests {
    use super::*;
    use iroh::SecretKey;

    fn temp_path(tag: &str) -> PathBuf {
        std::env::temp_dir().join(format!("murmer-allow-{tag}-{}.txt", std::process::id()))
    }

    #[test]
    fn roundtrip_file() {
        let path = temp_path("rt");
        let _ = std::fs::remove_file(&path);
        let a = SecretKey::generate().public();
        let b = SecretKey::generate().public();
        assert!(add_to_file(&path, a).unwrap());
        assert!(add_to_file(&path, b).unwrap());
        assert!(!add_to_file(&path, a).unwrap()); // already present
        let set = load_file(&path).unwrap();
        assert!(set.contains(&a) && set.contains(&b));
        assert!(remove_from_file(&path, &a).unwrap());
        let set = load_file(&path).unwrap();
        assert!(!set.contains(&a) && set.contains(&b));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn open_allows_everyone() {
        let allow = Allowlist::open();
        assert!(allow.is_allowed(&SecretKey::generate().public()));
        assert!(!allow.is_enforced());
        assert!(allow.subscribe_revocations().is_none());
    }

    #[test]
    fn missing_file_is_empty_not_error() {
        let path = temp_path("missing");
        let _ = std::fs::remove_file(&path);
        assert!(load_file(&path).unwrap().is_empty());
    }

    #[test]
    fn fingerprint_tracks_content_not_just_size() {
        let path = temp_path("fingerprint");
        let _ = std::fs::remove_file(&path);
        let a = SecretKey::generate().public();
        let b = SecretKey::generate().public();

        write_file(&path, &HashSet::from([a])).unwrap();
        let fp_a = file_fingerprint(&path);

        // Swap to a different key: same entry count (so same length), so an
        // mtime- or size-only check could miss it. The content fingerprint must
        // change, otherwise a key removal/swap would never trigger a reload.
        write_file(&path, &HashSet::from([b])).unwrap();
        let fp_b = file_fingerprint(&path);

        assert!(fp_a.is_some());
        assert_ne!(fp_a, fp_b, "a content change must change the fingerprint");
        let _ = std::fs::remove_file(&path);
    }
}
