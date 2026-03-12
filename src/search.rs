use std::sync::Arc;

use arrow_array::{Float32Array, StringArray};
use futures::TryStreamExt;
use lancedb::query::{ExecutableQuery, QueryBase};
use lancedb::DistanceType;

use crate::embed::Embedder;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SearchResult {
    pub text: String,
    pub file_path: String,
    pub heading: String,
    pub date: Option<String>,
    pub doc_type: Option<String>,
    pub tags: Option<Vec<String>>,
    pub project: Option<String>,
    pub score: f32,
}

#[derive(Clone)]
pub struct Searcher {
    db: lancedb::Connection,
    embedder: Arc<dyn Embedder>,
    model_name: String,
}

impl Searcher {
    pub fn new(db: lancedb::Connection, embedder: Arc<dyn Embedder>) -> Self {
        let model_name = embedder.model_name().to_string();
        Self {
            db,
            embedder,
            model_name,
        }
    }

    pub fn connection(&self) -> lancedb::Connection {
        self.db.clone()
    }

    pub async fn search(
        &self,
        query: &str,
        top_k: Option<usize>,
    ) -> anyhow::Result<Vec<SearchResult>> {
        let query = query.trim();
        // r[impl search.error.empty-query]
        if query.is_empty() {
            anyhow::bail!("query must not be empty");
        }

        // r[impl search.error.no-index]
        let table_names = self.db.table_names().execute().await?;
        if !table_names.contains(&"chunks".to_string()) {
            anyhow::bail!("index not found — run reindex first");
        }

        let table = self.db.open_table("chunks").execute().await?;
        let count = table.count_rows(None).await?;
        if count == 0 {
            anyhow::bail!("index not found — run reindex first");
        }

        let first_batch = table
            .query()
            .limit(1)
            .execute()
            .await?
            .try_next()
            .await?
            .ok_or_else(|| anyhow::anyhow!("no rows"))?;
        let indexed_model = first_batch
            .column_by_name("model_name")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .map(|a| a.value(0).to_string())
            .unwrap_or_default();
        // r[impl search.error.model-mismatch]
        if indexed_model != self.model_name {
            anyhow::bail!(
                "model mismatch: index was built with '{}', but current model is '{}'",
                indexed_model,
                self.model_name
            );
        }

        // r[impl search.embed-query]
        let query_embeddings = self.embedder.embed(&[query.to_string()])?;
        let query_vector = query_embeddings
            .into_iter()
            .next()
            .ok_or_else(|| anyhow::anyhow!("embedding failed"))?;

        // r[impl search.default-top-k]
        let top_k = top_k.unwrap_or(10);

        // r[impl search.similarity]
        let results = table
            .query()
            .nearest_to(query_vector.as_slice())?
            .distance_type(DistanceType::Cosine)
            .limit(top_k)
            .execute()
            .await?;

        let batches: Vec<arrow_array::RecordBatch> = results.try_collect().await?;

        let mut search_results = Vec::new();
        for batch in &batches {
            let texts = batch
                .column_by_name("content")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>())
                .unwrap();
            let file_paths = batch
                .column_by_name("file_path")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>())
                .unwrap();
            let headings = batch
                .column_by_name("heading")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>())
                .unwrap();
            let dates = batch
                .column_by_name("date")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>());
            let doc_types = batch
                .column_by_name("doc_type")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>());
            let tags_col = batch
                .column_by_name("tags")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>());
            let projects = batch
                .column_by_name("project")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>());
            let distances = batch
                .column_by_name("_distance")
                .and_then(|c| c.as_any().downcast_ref::<Float32Array>())
                .unwrap();

            for i in 0..batch.num_rows() {
                let tags = tags_col.and_then(|a| {
                    let s = a.value(i);
                    if s.is_empty() {
                        None
                    } else {
                        serde_json::from_str(s).ok()
                    }
                });
                // r[impl search.result.text] r[impl search.result.meta]
                search_results.push(SearchResult {
                    text: texts.value(i).to_string(),
                    file_path: file_paths.value(i).to_string(),
                    heading: headings.value(i).to_string(),
                    date: dates.and_then(|a| {
                        let s = a.value(i);
                        if s.is_empty() { None } else { Some(s.to_string()) }
                    }),
                    doc_type: doc_types.and_then(|a| {
                        let s = a.value(i);
                        if s.is_empty() { None } else { Some(s.to_string()) }
                    }),
                    tags,
                    project: projects.and_then(|a| {
                        let s = a.value(i);
                        if s.is_empty() { None } else { Some(s.to_string()) }
                    }),
                    score: distances.value(i),
                });
            }
        }

        Ok(search_results)
    }
}

// TODO: r[search.hybrid] — hybrid search (vector + FTS) when Rust LanceDB SDK supports it

#[cfg(test)]
pub(crate) async fn setup_indexed_db(
    brain_content: &[(&str, &str)],
    model_name: &str,
    dimension: usize,
) -> (
    tempfile::TempDir,
    tempfile::TempDir,
    crate::index::Indexer,
    Searcher,
) {
    setup_indexed_db_with_embedder(
        brain_content,
        Arc::new(crate::embed::MockEmbedder::new(dimension, model_name)),
    )
    .await
}

#[cfg(test)]
pub(crate) async fn setup_indexed_db_with_embedder(
    brain_content: &[(&str, &str)],
    embedder: Arc<dyn Embedder>,
) -> (
    tempfile::TempDir,
    tempfile::TempDir,
    crate::index::Indexer,
    Searcher,
) {
    use std::fs;

    let brain_dir = tempfile::TempDir::new().unwrap();
    let index_dir = tempfile::TempDir::new().unwrap();

    for (path, content) in brain_content {
        let full_path = brain_dir.path().join(path);
        fs::create_dir_all(full_path.parent().unwrap()).unwrap();
        fs::write(&full_path, content).unwrap();
    }

    let indexer = crate::index::Indexer::new(
        brain_dir.path().to_path_buf(),
        index_dir.path().to_path_buf(),
        embedder.clone(),
    )
    .await
    .unwrap();
    indexer.full_reindex().await.unwrap();

    let searcher = Searcher::new(indexer.connection(), embedder);

    (brain_dir, index_dir, indexer, searcher)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::embed::{HashEmbedder, MockEmbedder};
    use tempfile::TempDir;

    // r[verify search.result.text] r[verify search.result.meta]
    #[tokio::test]
    async fn search_returns_correct_fields() {
        let (_brain, _index, _indexer, searcher) = setup_indexed_db(
            &[(
                "doc.md",
                "---\ndate: 2024-03-01\ntype: note\ntags: [rust, lance]\nproject: brain\n---\n## Section One\nContent about foo.",
            )],
            "model-a",
            4,
        )
        .await;

        let results = searcher.search("foo", None).await.unwrap();

        assert_eq!(results.len(), 1);
        let r = &results[0];
        assert!(r.text.contains("Content about foo"), "text should contain chunk content");
        assert_eq!(r.file_path, "doc.md");
        assert_eq!(r.heading, "## Section One");
        assert_eq!(r.date.as_deref(), Some("2024-03-01"));
        assert_eq!(r.doc_type.as_deref(), Some("note"));
        assert_eq!(r.tags.as_ref().unwrap(), &["rust", "lance"]);
        assert_eq!(r.project.as_deref(), Some("brain"));
    }

    // r[verify search.similarity]
    #[tokio::test]
    async fn search_ranking_uses_cosine_similarity() {
        let embedder = Arc::new(HashEmbedder::new(32, "hash-model"));
        let (_brain, _index, _indexer, searcher) = setup_indexed_db_with_embedder(
            &[
                ("a.md", "## Alpha\nAlpha content here."),
                ("b.md", "## Beta\nBeta content here."),
                ("c.md", "## Gamma\nGamma content here."),
            ],
            embedder,
        )
        .await;

        // Query with the exact chunk content so hash-based vectors match perfectly.
        let results = searcher
            .search("## Alpha\nAlpha content here.", Some(3))
            .await
            .unwrap();

        assert_eq!(results.len(), 3);
        assert_eq!(
            results[0].heading, "## Alpha",
            "exact content match should rank first"
        );
        assert!(
            results[0].score <= results[1].score,
            "scores should be non-decreasing (lower = closer)"
        );
        assert!(
            results[1].score <= results[2].score,
            "scores should be non-decreasing (lower = closer)"
        );
    }

    // r[verify search.default-top-k]
    #[tokio::test]
    async fn search_default_top_k() {
        let mut files = Vec::new();
        for i in 0..15 {
            files.push((
                format!("doc{i}.md"),
                format!("## Section {i}\nContent {i}."),
            ));
        }
        let content: Vec<(&str, &str)> = files
            .iter()
            .map(|(a, b)| (a.as_str(), b.as_str()))
            .collect();
        let (_brain, _index, _indexer, searcher) = setup_indexed_db(&content, "model-a", 4).await;

        let results = searcher.search("query", None).await.unwrap();

        assert_eq!(results.len(), 10);
    }

    #[tokio::test]
    async fn search_custom_top_k() {
        let (_brain, _index, _indexer, searcher) = setup_indexed_db(
            &[
                ("a.md", "## A\nContent a."),
                ("b.md", "## B\nContent b."),
                ("c.md", "## C\nContent c."),
            ],
            "model-a",
            4,
        )
        .await;

        let results = searcher.search("query", Some(2)).await.unwrap();

        assert_eq!(results.len(), 2);
    }

    // r[verify search.error.empty-query]
    #[tokio::test]
    async fn search_empty_query_errors() {
        let (_brain, _index, _indexer, searcher) = setup_indexed_db(
            &[("doc.md", "## X\nContent.")],
            "model-a",
            4,
        )
        .await;

        let err = searcher.search("", None).await.unwrap_err();
        assert!(err.to_string().contains("empty"));
    }

    #[tokio::test]
    async fn search_whitespace_query_errors() {
        let (_brain, _index, _indexer, searcher) = setup_indexed_db(
            &[("doc.md", "## X\nContent.")],
            "model-a",
            4,
        )
        .await;

        let err = searcher.search("   ", None).await.unwrap_err();
        assert!(err.to_string().contains("empty"));
    }

    // r[verify search.error.no-index]
    #[tokio::test]
    async fn search_no_index_errors() {
        let index_dir = TempDir::new().unwrap();
        let db = lancedb::connect(index_dir.path().to_str().unwrap())
            .execute()
            .await
            .unwrap();

        let embedder = Arc::new(MockEmbedder::new(4, "model-a"));
        let searcher = Searcher::new(db, embedder);
        let err = searcher.search("query", None).await.unwrap_err();
        assert!(err.to_string().contains("reindex"));
    }

    // r[verify search.error.model-mismatch]
    #[tokio::test]
    async fn search_model_mismatch_errors() {
        let (_brain, _index, _indexer, searcher) = setup_indexed_db(
            &[("doc.md", "## X\nContent.")],
            "model-a",
            4,
        )
        .await;

        let searcher = Searcher::new(
            searcher.connection(),
            Arc::new(MockEmbedder::new(4, "model-b")),
        );
        let err = searcher.search("query", None).await.unwrap_err();
        assert!(err.to_string().contains("model mismatch"));
    }
}
