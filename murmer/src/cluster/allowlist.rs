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
                // Fail fast on a missing file: in Enforced mode an absent file
                // would load as "trust nobody" and silently reject every peer,
                // so a typo'd or unprovisioned path must not boot. A present-but-
                // empty file is a deliberate "start locked, hot-add later" state
                // and is allowed (load_file_opt -> Some(empty)).
                let initial = load_file_opt(&path)?.ok_or_else(|| {
                    ClusterError::AllowlistFile(format!(
                        "enforced allowlist file {} not found; refusing to start \
                         (a missing file would silently reject every peer)",
                        path.display()
                    ))
                })?;
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

    /// An enforced allowlist seeded with `set` and **no** background watcher.
    /// Lets tests exercise reload/revocation policy directly without the 1s
    /// timer or a real file.
    #[cfg(test)]
    fn enforced_for_test(set: HashSet<EndpointId>) -> Self {
        let (revoked_tx, _) = broadcast::channel(64);
        Self {
            inner: Arc::new(Inner {
                mode: Mode::Enforced {
                    set: RwLock::new(set),
                    revoked_tx,
                },
            }),
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
///
/// This collapses "absent" into "empty", which is the right startup default but
/// wrong for hot-reload — see [`load_file_opt`], which keeps the distinction so
/// the watcher can skip a transient gap instead of revoking every peer.
pub fn load_file(path: impl AsRef<Path>) -> Result<HashSet<EndpointId>, ClusterError> {
    Ok(load_file_opt(path)?.unwrap_or_default())
}

/// Like [`load_file`] but distinguishes a missing file (`Ok(None)`) from a
/// present, parseable one (`Ok(Some(set))`). The hot-reload watcher uses this to
/// tell "operator emptied the file" (present → apply, revoking removed peers)
/// from "file momentarily gone" (absent → skip, keep the current set). Applying
/// an empty set on a transient gap would revoke every peer and partition the
/// cluster (issue #7).
pub fn load_file_opt(path: impl AsRef<Path>) -> Result<Option<HashSet<EndpointId>>, ClusterError> {
    let path = path.as_ref();
    let text = match std::fs::read_to_string(path) {
        Ok(t) => t,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
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
    Ok(Some(set))
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
                        reload_once(&allowlist, &path);
                    }
                }
                _ = shutdown.cancelled() => break,
            }
        }
    });
}

/// React to one detected change of the allowlist file. Applies the new set when
/// the file is present and parseable; **skips** a transient missing file rather
/// than applying an empty set (which would revoke every peer and partition the
/// cluster, issue #7). A present-but-empty file is a deliberate revoke-all and
/// is applied. Extracted from the watcher loop so the policy is unit-testable
/// without the 1s timer.
fn reload_once(allowlist: &Allowlist, path: &Path) {
    match load_file_opt(path) {
        Ok(Some(next)) => {
            tracing::info!(path = %path.display(), count = next.len(), "allowlist reloaded");
            allowlist.apply_reload(next);
        }
        Ok(None) => tracing::warn!(
            path = %path.display(),
            "allowlist file missing on reload; keeping current set (no revocation)"
        ),
        Err(e) => tracing::error!("allowlist reload failed: {e}"),
    }
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
    fn enforced_missing_file_fails_fast() {
        // A typo'd / unprovisioned path in Enforced mode must refuse to start,
        // not boot a node that silently rejects every peer (issue #13).
        let path = temp_path("enforced-missing");
        let _ = std::fs::remove_file(&path);
        let shutdown = CancellationToken::new();
        let result = Allowlist::new(AllowlistMode::Enforced(path), shutdown);
        assert!(
            result.is_err(),
            "enforced mode with a missing file must fail fast"
        );
    }

    #[tokio::test]
    async fn enforced_present_empty_file_starts() {
        // A present-but-empty file is a deliberate "start locked, hot-add later"
        // state and must still boot (distinct from the missing-file case above).
        let path = temp_path("enforced-present-empty");
        std::fs::write(&path, "# intentionally empty\n").unwrap();
        let shutdown = CancellationToken::new();
        let allow = Allowlist::new(AllowlistMode::Enforced(path.clone()), shutdown.clone())
            .expect("a present empty file is a valid locked allowlist");
        assert!(allow.is_enforced());
        shutdown.cancel(); // stop the spawned watcher
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn transient_missing_file_keeps_set_no_revocation() {
        // A file that momentarily reads as missing (delete-then-recreate during
        // a deploy) must NOT revoke every peer — that would partition the
        // cluster from a 200ms gap (issue #7).
        let a = SecretKey::generate().public();
        let b = SecretKey::generate().public();
        let allow = Allowlist::enforced_for_test(HashSet::from([a, b]));
        let mut rev = allow.subscribe_revocations().unwrap();

        let path = temp_path("transient-missing");
        let _ = std::fs::remove_file(&path); // ensure absent

        reload_once(&allow, &path);

        assert!(
            allow.is_allowed(&a) && allow.is_allowed(&b),
            "a transient missing file must not drop peers"
        );
        assert!(
            rev.try_recv().is_err(),
            "a transient missing file must broadcast no revocations"
        );
    }

    #[test]
    fn present_empty_file_revokes_all() {
        // An operator who intentionally empties a *present* file does mean
        // "revoke everyone" — that path is preserved.
        let a = SecretKey::generate().public();
        let allow = Allowlist::enforced_for_test(HashSet::from([a]));
        let mut rev = allow.subscribe_revocations().unwrap();

        let path = temp_path("present-empty");
        std::fs::write(&path, "# intentionally empty\n").unwrap();

        reload_once(&allow, &path);

        assert!(
            !allow.is_allowed(&a),
            "an intentionally emptied present file revokes everyone"
        );
        assert_eq!(rev.try_recv().ok(), Some(a));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn reload_revokes_only_removed_id() {
        let a = SecretKey::generate().public();
        let b = SecretKey::generate().public();
        let allow = Allowlist::enforced_for_test(HashSet::from([a, b]));
        let mut rev = allow.subscribe_revocations().unwrap();

        let path = temp_path("partial-reload");
        write_file(&path, &HashSet::from([a])).unwrap(); // b removed

        reload_once(&allow, &path);

        assert!(allow.is_allowed(&a), "a is still allowed");
        assert!(!allow.is_allowed(&b), "b was removed and revoked");
        assert_eq!(rev.try_recv().ok(), Some(b));
        assert!(
            rev.try_recv().is_err(),
            "only the genuinely removed id is revoked"
        );
        let _ = std::fs::remove_file(&path);
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
