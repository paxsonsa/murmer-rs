//! Node secret-key persistence.
//!
//! With iroh, a node's identity is its [`EndpointId`] (an ed25519 public key)
//! derived from a [`SecretKey`]. That key **must** survive restarts: the
//! `EndpointId` is what other nodes put in their allowlists, so a key that
//! regenerates on every boot would invalidate every allowlist entry.
//!
//! [`load_or_generate`] reads the key from disk if present, otherwise generates
//! a fresh one and persists it (raw 32 bytes, `0600` on unix — same posture as
//! an SSH private key).

use std::path::Path;

use iroh::{EndpointId, SecretKey};

use super::error::ClusterError;

/// Load the node secret key from `path`, or generate and persist a new one if
/// the file does not exist.
///
/// The on-disk format is the raw 32 secret-key bytes. On unix the file is
/// created with `0600` permissions.
pub fn load_or_generate(path: impl AsRef<Path>) -> Result<SecretKey, ClusterError> {
    let path = path.as_ref();
    if path.exists() {
        load(path)
    } else {
        let key = SecretKey::generate();
        persist(path, &key)?;
        tracing::info!(
            path = %path.display(),
            endpoint_id = %key.public(),
            "generated new node identity key"
        );
        Ok(key)
    }
}

/// Load a secret key from `path`. Errors if the file is missing or not exactly
/// 32 bytes.
pub fn load(path: impl AsRef<Path>) -> Result<SecretKey, ClusterError> {
    let path = path.as_ref();
    let bytes = std::fs::read(path)
        .map_err(|e| ClusterError::KeyFile(format!("read {}: {e}", path.display())))?;
    let arr: [u8; 32] = bytes.as_slice().try_into().map_err(|_| {
        ClusterError::KeyFile(format!(
            "{}: expected 32-byte key, found {} bytes",
            path.display(),
            bytes.len()
        ))
    })?;
    Ok(SecretKey::from_bytes(&arr))
}

/// Write `key` to `path` as raw bytes, creating parent directories as needed.
pub fn persist(path: impl AsRef<Path>, key: &SecretKey) -> Result<(), ClusterError> {
    let path = path.as_ref();
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent)
            .map_err(|e| ClusterError::KeyFile(format!("create dir {}: {e}", parent.display())))?;
    }
    std::fs::write(path, key.to_bytes())
        .map_err(|e| ClusterError::KeyFile(format!("write {}: {e}", path.display())))?;
    restrict_permissions(path)?;
    Ok(())
}

/// Derive the public `EndpointId` for a key file without exposing the secret.
pub fn endpoint_id_of(path: impl AsRef<Path>) -> Result<EndpointId, ClusterError> {
    Ok(load(path)?.public())
}

#[cfg(unix)]
fn restrict_permissions(path: &Path) -> Result<(), ClusterError> {
    use std::os::unix::fs::PermissionsExt;
    let perms = std::fs::Permissions::from_mode(0o600);
    std::fs::set_permissions(path, perms)
        .map_err(|e| ClusterError::KeyFile(format!("chmod {}: {e}", path.display())))
}

#[cfg(not(unix))]
fn restrict_permissions(_path: &Path) -> Result<(), ClusterError> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_then_load_roundtrips_same_identity() {
        let dir = std::env::temp_dir().join(format!("murmer-key-test-{}", std::process::id()));
        let path = dir.join("node.key");
        let _ = std::fs::remove_file(&path);

        let key1 = load_or_generate(&path).expect("generate");
        // Second call loads the persisted key — same EndpointId.
        let key2 = load_or_generate(&path).expect("load");
        assert_eq!(key1.public(), key2.public());

        // endpoint_id_of agrees without loading the secret elsewhere.
        assert_eq!(endpoint_id_of(&path).unwrap(), key1.public());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn wrong_length_key_file_errors() {
        let dir = std::env::temp_dir().join(format!("murmer-key-bad-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("bad.key");
        std::fs::write(&path, b"too-short").unwrap();
        assert!(load(&path).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
