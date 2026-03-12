use std::path::PathBuf;
use std::sync::Arc;

use cl_lance_mcp::embed::FastEmbedder;
use cl_lance_mcp::index::Indexer;
use cl_lance_mcp::leader::LeaderGuard;
use cl_lance_mcp::search::Searcher;
use cl_lance_mcp::server::BrainServer;
use rmcp::ServiceExt;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();

    let brain_root = std::env::var("BRAIN_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|_| std::env::current_dir().unwrap());

    let index_dir = std::env::var("BRAIN_INDEX_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| brain_root.join(".brain-index"));

    // r[impl daemon.lock]
    let leader = LeaderGuard::try_acquire(&index_dir)?;
    let is_leader = leader.is_some();

    let embedder = Arc::new(FastEmbedder::default_model()?);
    let indexer = Indexer::new(brain_root.clone(), index_dir.clone(), embedder.clone()).await?;
    let searcher = Searcher::new(indexer.connection(), embedder);

    if let Some(ref guard) = leader {
        // r[impl daemon.leader.watcher] r[impl index.watch]
        indexer.spawn_watcher()?;
        // r[impl daemon.leader.startup-reindex] r[impl index.startup-reindex]
        indexer.incremental_reindex(&[]).await?;
        guard.serve_reindex(indexer.clone(), brain_root.clone());
    } else {
        tracing::info!("running as follower — reindex forwarded to leader");
    }

    let server = BrainServer::new(indexer, searcher, brain_root, index_dir, is_leader);

    // r[impl mcp.transport.stdio]
    let service = server.serve(rmcp::transport::io::stdio()).await?;
    service.waiting().await?;

    drop(leader);
    Ok(())
}
