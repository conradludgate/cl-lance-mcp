use std::path::PathBuf;

use rmcp::{
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::{ServerCapabilities, ServerInfo},
    tool, tool_handler, tool_router, ServerHandler,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::index::Indexer;
use crate::search::Searcher;

#[derive(Clone)]
pub struct BrainServer {
    indexer: Indexer,
    searcher: Searcher,
    brain_root: PathBuf,
    tool_router: ToolRouter<Self>,
}

// r[impl mcp.tool.search.top-k-param]
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct SearchParams {
    pub query: String,
    #[schemars(description = "Maximum number of results to return")]
    pub top_k: Option<usize>,
}

// r[impl mcp.tool.reindex.path-param]
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct ReindexParams {
    #[schemars(description = "Optional path (relative to brain root) to scope reindexing")]
    pub path: Option<String>,
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for BrainServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_instructions("Brain RAG search server")
    }
}

#[tool_router]
impl BrainServer {
    pub fn new(indexer: Indexer, searcher: Searcher, brain_root: PathBuf) -> Self {
        Self {
            indexer,
            searcher,
            brain_root,
            tool_router: Self::tool_router(),
        }
    }

    // r[impl mcp.tool.search]
    #[tool(description = "Search the brain index by semantic similarity")]
    pub async fn search(
        &self,
        params: Parameters<SearchParams>,
    ) -> Result<String, String> {
        let results = self
            .searcher
            .search(&params.0.query, params.0.top_k)
            .await
            .map_err(|e| e.to_string())?;
        serde_json::to_string_pretty(&results).map_err(|e| e.to_string())
    }

    // r[impl mcp.tool.reindex]
    #[tool(description = "Reindex the brain. Omit path for full reindex, or provide a path to scope.")]
    pub async fn reindex(&self, params: Parameters<ReindexParams>) -> Result<String, String> {
        if let Some(ref p) = params.0.path {
            let full = self.brain_root.join(p);
            // r[impl mcp.tool.reindex.path-invalid]
            if !full.exists() {
                return Err(format!("path does not exist: {}", full.display()));
            }
            let full_canon = full.canonicalize().map_err(|e| e.to_string())?;
            let root_canon = self.brain_root.canonicalize().map_err(|e| e.to_string())?;
            if !full_canon.starts_with(&root_canon) {
                return Err("path is outside brain root".to_string());
            }
            let stats = self
                .indexer
                .incremental_reindex(&[PathBuf::from(p)])
                .await
                .map_err(|e| e.to_string())?;
            Ok(format!(
                "files_processed: {}, chunks_indexed: {}",
                stats.files_processed, stats.chunks_indexed
            ))
        } else {
            let stats = self
                .indexer
                .full_reindex()
                .await
                .map_err(|e| e.to_string())?;
            Ok(format!(
                "files_processed: {}, chunks_indexed: {}",
                stats.files_processed, stats.chunks_indexed
            ))
        }
    }

    // r[impl mcp.tool.catalog] r[impl mcp.tool.catalog.list]
    #[tool(description = "List all indexed documents with file path, date, type, tags, and project")]
    pub async fn catalog(&self) -> Result<String, String> {
        let entries = self
            .indexer
            .catalog()
            .await
            .map_err(|e| e.to_string())?;
        serde_json::to_string_pretty(&entries).map_err(|e| e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::search::setup_indexed_db;

    async fn setup_server() -> (BrainServer, tempfile::TempDir, tempfile::TempDir) {
        let (brain_dir, index_dir, indexer, searcher) = setup_indexed_db(
            &[("test.md", "## Section\nTest content.")],
            "test-model",
            4,
        )
        .await;

        let server = BrainServer::new(indexer, searcher, brain_dir.path().to_path_buf());
        (server, brain_dir, index_dir)
    }

    // r[verify mcp.tool.search] r[verify mcp.tool.reindex] r[verify mcp.tool.catalog]
    #[tokio::test]
    async fn brain_server_new_creates_successfully() {
        let (server, _brain, _index) = setup_server().await;
        let tools = server.tool_router.list_all();
        assert_eq!(tools.len(), 3);
        let names: Vec<_> = tools.iter().map(|t| t.name.as_ref()).collect();
        assert!(names.contains(&"search"));
        assert!(names.contains(&"reindex"));
        assert!(names.contains(&"catalog"));
    }

    // r[verify mcp.tool.search.top-k-param]
    #[tokio::test]
    async fn search_tool_top_k_is_optional() {
        let (server, _brain, _index) = setup_server().await;

        let with = Parameters(SearchParams {
            query: "test".to_string(),
            top_k: Some(5),
        });
        let result = server.search(with).await.unwrap();
        let parsed: Vec<crate::search::SearchResult> = serde_json::from_str(&result).unwrap();
        assert!(!parsed.is_empty());

        let without = Parameters(SearchParams {
            query: "test".to_string(),
            top_k: None,
        });
        let result = server.search(without).await.unwrap();
        let parsed: Vec<crate::search::SearchResult> = serde_json::from_str(&result).unwrap();
        assert!(!parsed.is_empty());
    }

    // r[verify mcp.tool.catalog.list]
    #[tokio::test]
    async fn catalog_tool_returns_metadata() {
        let (brain_dir, index_dir, indexer, searcher) = setup_indexed_db(
            &[(
                "doc.md",
                "---\ndate: 2024-06-01\ntype: report\ntags: [x, y]\nproject: demo\n---\n## Heading\nBody.",
            )],
            "test-model",
            4,
        )
        .await;

        let server = BrainServer::new(indexer, searcher, brain_dir.path().to_path_buf());
        let result = server.catalog().await.unwrap();
        let parsed: Vec<crate::index::CatalogEntry> = serde_json::from_str(&result).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].file_path, "doc.md");
        assert_eq!(parsed[0].date.as_deref(), Some("2024-06-01"));
        assert_eq!(parsed[0].doc_type.as_deref(), Some("report"));
        assert_eq!(parsed[0].tags.as_ref().unwrap(), &["x", "y"]);
        assert_eq!(parsed[0].project.as_deref(), Some("demo"));
        drop(index_dir);
    }

    // r[verify mcp.tool.reindex.path-param]
    #[tokio::test]
    async fn reindex_tool_full_succeeds() {
        let (server, _brain, _index) = setup_server().await;
        let params = Parameters(ReindexParams { path: None });
        let result = server.reindex(params).await.unwrap();
        assert!(result.contains("files_processed"));
        assert!(result.contains("chunks_indexed"));
    }

    // r[verify mcp.tool.reindex.path-invalid]
    #[tokio::test]
    async fn reindex_tool_invalid_path_errors() {
        let (server, _brain, _index) = setup_server().await;
        let params = Parameters(ReindexParams {
            path: Some("nonexistent.md".to_string()),
        });
        let err = server.reindex(params).await.unwrap_err();
        assert!(err.contains("does not exist"));

        let params = Parameters(ReindexParams {
            path: Some("..".to_string()),
        });
        let err = server.reindex(params).await.unwrap_err();
        assert!(err.contains("outside"));
    }
}
