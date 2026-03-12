use std::fs::{self, File};
use std::path::{Path, PathBuf};

use anyhow::Result;
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixListener;

use crate::index::{IndexStats, Indexer};

#[derive(Debug, Serialize, Deserialize)]
pub struct ReindexRequest {
    pub path: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReindexResponse {
    Ok(IndexStats),
    Err(String),
}

/// Held by the leader instance to maintain the exclusive flock.
/// The lock is released when this guard is dropped.
pub struct LeaderGuard {
    _lock_file: File,
    sock_path: PathBuf,
}

impl LeaderGuard {
    // r[impl daemon.lock]
    pub fn try_acquire(index_dir: &Path) -> Result<Option<Self>> {
        fs::create_dir_all(index_dir)?;
        let lock_path = index_dir.join("brain.lock");
        let lock_file = File::create(&lock_path)?;

        if lock_file.try_lock_exclusive().is_err() {
            return Ok(None);
        }

        let sock_path = index_dir.join("brain.sock");
        Ok(Some(Self {
            _lock_file: lock_file,
            sock_path,
        }))
    }

    /// Spawn a background task that accepts reindex requests from followers.
    // r[impl daemon.leader.socket]
    pub fn serve_reindex(&self, indexer: Indexer, brain_root: PathBuf) {
        let sock_path = self.sock_path.clone();

        // r[impl daemon.socket.cleanup]
        let _ = fs::remove_file(&sock_path);
        let listener = UnixListener::bind(&sock_path)
            .unwrap_or_else(|e| panic!("failed to bind {}: {e}", sock_path.display()));

        tokio::spawn(async move {
            loop {
                let (stream, _) = match listener.accept().await {
                    Ok(conn) => conn,
                    Err(e) => {
                        tracing::warn!(error = %e, "socket accept failed");
                        continue;
                    }
                };

                let indexer = indexer.clone();
                let brain_root = brain_root.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_connection(stream, &indexer, &brain_root).await {
                        tracing::warn!(error = %e, "reindex forwarding failed");
                    }
                });
            }
        });

        tracing::info!(path = %sock_path.display(), "reindex socket listening");
    }
}

impl Drop for LeaderGuard {
    fn drop(&mut self) {
        // r[impl daemon.socket.cleanup]
        let _ = fs::remove_file(&self.sock_path);
    }
}

async fn handle_connection(
    stream: tokio::net::UnixStream,
    indexer: &Indexer,
    brain_root: &Path,
) -> Result<()> {
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let mut line = String::new();
    reader.read_line(&mut line).await?;

    let req: ReindexRequest = serde_json::from_str(line.trim())?;

    let response = match execute_reindex(indexer, brain_root, &req).await {
        Ok(stats) => ReindexResponse::Ok(stats),
        Err(e) => ReindexResponse::Err(e.to_string()),
    };

    let mut resp_bytes = serde_json::to_vec(&response)?;
    resp_bytes.push(b'\n');
    writer.write_all(&resp_bytes).await?;
    Ok(())
}

async fn execute_reindex(
    indexer: &Indexer,
    brain_root: &Path,
    req: &ReindexRequest,
) -> Result<IndexStats> {
    match &req.path {
        Some(p) => {
            let full = brain_root.join(p);
            anyhow::ensure!(full.exists(), "path does not exist: {}", full.display());
            let full_canon = full.canonicalize()?;
            let root_canon = brain_root.canonicalize()?;
            anyhow::ensure!(
                full_canon.starts_with(&root_canon),
                "path is outside brain root"
            );
            indexer.incremental_reindex(&[PathBuf::from(p)]).await
        }
        None => indexer.full_reindex().await,
    }
}

/// Used by followers to forward a reindex request to the leader.
// r[impl daemon.follower.reindex-forward]
pub async fn forward_reindex(index_dir: &Path, request: &ReindexRequest) -> Result<IndexStats> {
    let sock_path = index_dir.join("brain.sock");

    // r[impl daemon.follower.reindex-unavailable]
    let stream = tokio::net::UnixStream::connect(&sock_path)
        .await
        .map_err(|_| anyhow::anyhow!("leader instance is unavailable — cannot forward reindex"))?;

    let (reader, mut writer) = stream.into_split();

    let mut req_bytes = serde_json::to_vec(request)?;
    req_bytes.push(b'\n');
    writer.write_all(&req_bytes).await?;
    writer.shutdown().await?;

    let mut reader = BufReader::new(reader);
    let mut line = String::new();
    reader.read_line(&mut line).await?;

    let response: ReindexResponse = serde_json::from_str(line.trim())?;
    match response {
        ReindexResponse::Ok(stats) => Ok(stats),
        ReindexResponse::Err(msg) => Err(anyhow::anyhow!(msg)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // r[verify daemon.lock]
    #[test]
    fn leader_acquires_lock_follower_does_not() {
        let dir = tempfile::tempdir().unwrap();
        let guard = LeaderGuard::try_acquire(dir.path()).unwrap();
        assert!(guard.is_some(), "first caller should become leader");

        let second = LeaderGuard::try_acquire(dir.path()).unwrap();
        assert!(second.is_none(), "second caller should be follower");

        drop(guard);

        let third = LeaderGuard::try_acquire(dir.path()).unwrap();
        assert!(
            third.is_some(),
            "after leader drops, a new leader can be elected"
        );
    }

    // r[verify daemon.socket.cleanup]
    #[test]
    fn leader_drop_removes_socket() {
        let dir = tempfile::tempdir().unwrap();
        let guard = LeaderGuard::try_acquire(dir.path()).unwrap().unwrap();
        let sock_path = dir.path().join("brain.sock");

        File::create(&sock_path).unwrap();
        assert!(sock_path.exists());

        drop(guard);
        assert!(!sock_path.exists());
    }

    // r[verify daemon.follower.reindex-unavailable]
    #[tokio::test]
    async fn forward_reindex_fails_when_no_leader() {
        let dir = tempfile::tempdir().unwrap();
        let req = ReindexRequest { path: None };
        let err = forward_reindex(dir.path(), &req).await.unwrap_err();
        assert!(
            err.to_string().contains("unavailable"),
            "should report leader unavailable, got: {err}"
        );
    }

    // r[verify daemon.leader.socket] r[verify daemon.follower.reindex-forward]
    #[tokio::test]
    async fn follower_forwards_reindex_to_leader() {
        use crate::embed::MockEmbedder;
        use std::sync::Arc;

        let brain_dir = tempfile::tempdir().unwrap();
        let index_dir = tempfile::tempdir().unwrap();

        std::fs::write(brain_dir.path().join("hello.md"), "## Hello\nWorld.").unwrap();

        let embedder = Arc::new(MockEmbedder::new(4, "test"));
        let indexer = Indexer::new(
            brain_dir.path().to_path_buf(),
            index_dir.path().to_path_buf(),
            embedder,
        )
        .await
        .unwrap();

        let guard = LeaderGuard::try_acquire(index_dir.path()).unwrap().unwrap();
        guard.serve_reindex(indexer, brain_dir.path().to_path_buf());

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let req = ReindexRequest { path: None };
        let stats = forward_reindex(index_dir.path(), &req).await.unwrap();
        assert!(stats.files_processed > 0);
        assert!(stats.chunks_indexed > 0);

        drop(guard);
    }
}
