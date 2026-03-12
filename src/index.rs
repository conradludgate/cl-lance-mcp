use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use arrow_array::Array;
use arrow_array::{RecordBatch, RecordBatchIterator};
use arrow_schema::{DataType, Field, Schema};
use futures::TryStreamExt;
use lancedb::database::CreateTableMode;
use lancedb::index::Index;
use lancedb::query::{ExecutableQuery, QueryBase, Select};
use lancedb::{connect, Table};
use blake3::Hasher;
use walkdir::WalkDir;

use crate::chunk::{chunk_file, Chunk};
use crate::embed::Embedder;

#[derive(Debug, Clone)]
pub struct IndexStats {
    pub files_processed: usize,
    pub chunks_indexed: usize,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CatalogEntry {
    pub file_path: String,
    pub date: Option<String>,
    pub doc_type: Option<String>,
    pub tags: Option<Vec<String>>,
    pub project: Option<String>,
}

#[derive(Clone)]
pub struct Indexer {
    db: lancedb::Connection,
    embedder: Arc<dyn Embedder>,
    brain_root: PathBuf,
    #[allow(dead_code)]
    index_dir: PathBuf,
}

fn content_hash(content: &[u8]) -> String {
    blake3::hash(content).to_hex().to_string()
}

fn chunk_id(file_path: &str, heading: &str) -> String {
    let mut hasher = Hasher::new();
    hasher.update(file_path.as_bytes());
    hasher.update(b"::");
    hasher.update(heading.as_bytes());
    hasher.finalize().to_hex().to_string()
}

// r[impl index.scan.md-only] r[impl index.scan.recursive]
async fn scan_md_files(root: PathBuf) -> Vec<PathBuf> {
    let root = root.clone();
    tokio::task::spawn_blocking(move || {
        WalkDir::new(&root)
            .follow_links(false)
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().is_file())
            .filter(|e| e.path().extension().is_some_and(|ext| ext == "md"))
            .map(|e| e.into_path())
            .collect()
    })
    .await
    .unwrap_or_default()
}

fn escape_sql_string(s: &str) -> String {
    s.replace('\\', "\\\\").replace('\'', "''")
}

impl Indexer {
    // r[impl index.storage.dir-config]
    pub async fn new(
        brain_root: PathBuf,
        index_dir: PathBuf,
        embedder: Arc<dyn Embedder>,
    ) -> Result<Self> {
        let brain_root = brain_root.canonicalize().unwrap_or(brain_root);
        let uri = index_dir
            .to_str()
            .ok_or_else(|| anyhow::anyhow!("index_dir is not valid UTF-8"))?;
        let db = connect(uri).execute().await?;
        Ok(Self {
            db,
            embedder,
            brain_root,
            index_dir,
        })
    }

    pub fn model_name(&self) -> &str {
        self.embedder.model_name()
    }

    pub fn connection(&self) -> lancedb::Connection {
        self.db.clone()
    }

    // r[impl index.full]
    pub async fn full_reindex(&self) -> Result<IndexStats> {
        let dimension = self.embedder.dimension();
        let model_name = self.embedder.model_name().to_string();

        let mut all_chunks: Vec<Chunk> = Vec::new();
        let mut all_content_hashes: Vec<String> = Vec::new();

        // r[impl index.scan.root]
        for path in scan_md_files(self.brain_root.clone()).await {
            let content = tokio::fs::read_to_string(&path).await?;
            let rel_path = path
                .strip_prefix(&self.brain_root)
                .unwrap_or(&path)
                .to_string_lossy()
                .replace('\\', "/");
            let chunks = chunk_file(&rel_path, &content);
            let hash = content_hash(content.as_bytes());
            for _ in &chunks {
                all_content_hashes.push(hash.clone());
            }
            all_chunks.extend(chunks);
        }

        let files_processed = all_chunks
            .iter()
            .map(|c| c.file_path.as_str())
            .collect::<std::collections::HashSet<_>>()
            .len();

        let chunks_to_index = self.embed_chunks(all_chunks, all_content_hashes)?;
        let chunks_indexed = chunks_to_index.len();

        // r[impl index.storage.lance]
        let schema = Self::chunks_schema(dimension);
        let batch = if chunks_to_index.is_empty() {
            Self::empty_batch(&schema, dimension)?
        } else {
            Self::chunks_to_record_batch(&chunks_to_index, &schema, dimension, &model_name)?
        };
        let batches =
            RecordBatchIterator::new(vec![Ok(batch)].into_iter(), Arc::new(schema.clone()));
        let table = self
            .db
            .create_table("chunks", Box::new(batches))
            .mode(CreateTableMode::Overwrite)
            .execute()
            .await?;

        // r[impl search.hybrid]
        self.create_fts_index(&table).await?;

        Ok(IndexStats {
            files_processed,
            chunks_indexed,
        })
    }

    // r[impl index.incremental]
    pub async fn incremental_reindex(&self, paths: &[PathBuf]) -> Result<IndexStats> {
        let dimension = self.embedder.dimension();
        let model_name = self.embedder.model_name().to_string();

        let md_files = if paths.is_empty() {
            scan_md_files(self.brain_root.clone()).await
        } else {
            let mut files = Vec::new();
            for p in paths {
                let abs = if p.is_absolute() {
                    p.clone()
                } else {
                    self.brain_root.join(p)
                };
                if abs.is_dir() {
                    files.extend(scan_md_files(abs).await);
                } else if abs.extension().is_some_and(|ext| ext == "md") && abs.exists() {
                    files.push(abs);
                }
            }
            files
        };

        let mut to_process: Vec<(PathBuf, String, Vec<Chunk>)> = Vec::new();
        for p in md_files {
            let content = match tokio::fs::read_to_string(&p).await {
                Ok(c) => c,
                Err(_) => continue,
            };
            let rel_path = p
                .strip_prefix(&self.brain_root)
                .unwrap_or(&p)
                .to_string_lossy()
                .replace('\\', "/");
            let chunks = chunk_file(&rel_path, &content);
            let hash = content_hash(content.as_bytes());
            to_process.push((p, hash, chunks));
        }

        let table = self.open_or_create_table(dimension).await?;

        let mut files_processed = 0;
        let mut chunks_indexed = 0;

        for (_, file_hash, chunks) in &to_process {
            let stored_hash = self.get_stored_hash(&table, &chunks[0].file_path).await?;
            if stored_hash.as_deref() == Some(file_hash.as_str()) {
                continue;
            }

            // r[impl index.upsert]
            table
                .delete(&format!(
                    "file_path = '{}'",
                    escape_sql_string(&chunks[0].file_path)
                ))
                .await?;

            let hashes: Vec<String> = chunks.iter().map(|_| file_hash.clone()).collect();
            let indexed = self.embed_chunks(chunks.clone(), hashes)?;

            if !indexed.is_empty() {
                files_processed += 1;
                chunks_indexed += indexed.len();
                let schema = Self::chunks_schema(dimension);
                let batch =
                    Self::chunks_to_record_batch(&indexed, &schema, dimension, &model_name)?;
                let batches = RecordBatchIterator::new(
                    vec![Ok(batch)].into_iter(),
                    Arc::new(schema.clone()),
                );
                table.add(Box::new(batches)).execute().await?;
            }
        }

        // r[impl index.stale-removal]
        if paths.is_empty() {
            let indexed_paths: std::collections::HashSet<_> = to_process
                .iter()
                .map(|(_, _, c)| c[0].file_path.as_str())
                .collect();
            let stored_paths = self.list_stored_file_paths(&table).await?;
            for stored in stored_paths {
                if !indexed_paths.contains(stored.as_str()) {
                    table
                        .delete(&format!("file_path = '{}'", escape_sql_string(&stored)))
                        .await?;
                }
            }
        }

        if files_processed > 0 {
            self.create_fts_index(&table).await?;
        }

        Ok(IndexStats {
            files_processed,
            chunks_indexed,
        })
    }

    // r[impl index.watch]
    pub fn spawn_watcher(&self) -> Result<()> {
        use notify_debouncer_mini::{new_debouncer, DebounceEventResult};
        use std::time::Duration;

        let indexer = self.clone();
        let brain_root = self.brain_root.clone();
        let index_dir = self.index_dir.canonicalize().unwrap_or(self.index_dir.clone());
        let rt = tokio::runtime::Handle::current();

        let mut debouncer = new_debouncer(
            Duration::from_secs(2),
            move |result: DebounceEventResult| {
                let paths: Vec<PathBuf> = match result {
                    Ok(events) => events
                        .into_iter()
                        .filter(|e| !e.path.starts_with(&index_dir))
                        .filter(|e| {
                            e.path
                                .extension()
                                .is_some_and(|ext| ext == "md")
                        })
                        .map(|e| e.path)
                        .collect(),
                    Err(e) => {
                        tracing::warn!(error = %e, "file watcher error");
                        return;
                    }
                };

                if paths.is_empty() {
                    return;
                }

                let indexer = indexer.clone();
                rt.spawn(async move {
                    if let Err(e) = indexer.incremental_reindex(&paths).await {
                        tracing::warn!(error = %e, "watcher-triggered reindex failed");
                    }
                });
            },
        )?;

        debouncer.watcher().watch(
            &brain_root,
            notify::RecursiveMode::Recursive,
        )?;

        // Leak the debouncer so it lives for the process lifetime.
        // The MCP server runs until the process exits, so no cleanup is needed.
        std::mem::forget(debouncer);

        tracing::info!(path = %brain_root.display(), "file watcher started");
        Ok(())
    }

    pub async fn catalog(&self) -> Result<Vec<CatalogEntry>> {
        let table = self.open_or_create_table(self.embedder.dimension()).await?;

        let stream = table
            .query()
            .select(Select::columns(&[
                "file_path",
                "date",
                "doc_type",
                "tags",
                "project",
            ]))
            .execute()
            .await?;
        let batches: Vec<RecordBatch> = stream.try_collect().await?;

        let mut seen = std::collections::HashSet::new();
        let mut entries = Vec::new();
        for batch in batches {
            let file_path_arr = batch
                .column_by_name("file_path")
                .ok_or_else(|| anyhow::anyhow!("missing file_path column"))?;
            let file_paths = arrow_array::cast::as_string_array(file_path_arr);
            let date_arr = batch.column_by_name("date");
            let doc_type_arr = batch.column_by_name("doc_type");
            let tags_arr = batch.column_by_name("tags");
            let project_arr = batch.column_by_name("project");

            for i in 0..batch.num_rows() {
                let fp = file_paths.value(i).to_string();
                if !seen.insert(fp.clone()) {
                    continue;
                }
                let date = date_arr.and_then(|a| {
                    let arr = arrow_array::cast::as_string_array(a);
                    if arr.is_null(i) {
                        None
                    } else {
                        let s = arr.value(i).to_string();
                        if s.is_empty() {
                            None
                        } else {
                            Some(s)
                        }
                    }
                });
                let doc_type = doc_type_arr.and_then(|a| {
                    let arr = arrow_array::cast::as_string_array(a);
                    if arr.is_null(i) {
                        None
                    } else {
                        let s = arr.value(i).to_string();
                        if s.is_empty() {
                            None
                        } else {
                            Some(s)
                        }
                    }
                });
                let tags = tags_arr.and_then(|a| {
                    let arr = arrow_array::cast::as_string_array(a);
                    if arr.is_null(i) {
                        None
                    } else {
                        let s = arr.value(i).to_string();
                        if s.is_empty() {
                            None
                        } else {
                            serde_json::from_str(&s).ok()
                        }
                    }
                });
                let project = project_arr.and_then(|a| {
                    let arr = arrow_array::cast::as_string_array(a);
                    if arr.is_null(i) {
                        None
                    } else {
                        let s = arr.value(i).to_string();
                        if s.is_empty() {
                            None
                        } else {
                            Some(s)
                        }
                    }
                });
                entries.push(CatalogEntry {
                    file_path: fp,
                    date,
                    doc_type,
                    tags,
                    project,
                });
            }
        }
        entries.sort_by(|a, b| a.file_path.cmp(&b.file_path));
        Ok(entries)
    }

    /// Batch-embed chunks; on batch failure, fall back to per-chunk embedding.
    /// Returns only successfully embedded chunks, positionally aligned.
    fn embed_chunks(
        &self,
        chunks: Vec<Chunk>,
        content_hashes: Vec<String>,
    ) -> Result<Vec<(Chunk, Vec<f32>, String)>> {
        if chunks.is_empty() {
            return Ok(vec![]);
        }

        let texts: Vec<String> = chunks.iter().map(|c| c.content.clone()).collect();
        match self.embedder.embed(&texts) {
            Ok(vectors) => {
                Ok(chunks
                    .into_iter()
                    .zip(vectors)
                    .zip(content_hashes)
                    .map(|((c, v), h)| (c, v, h))
                    .collect())
            }
            Err(batch_err) => {
                tracing::warn!(error = %batch_err, "batch embed failed, falling back to per-chunk");
                let mut result = Vec::new();
                for (chunk, hash) in chunks.into_iter().zip(content_hashes) {
                    match self.embedder.embed(std::slice::from_ref(&chunk.content)) {
                        Ok(v) => {
                            if let Some(vec) = v.into_iter().next() {
                                result.push((chunk, vec, hash));
                            }
                        }
                        // r[impl index.error.embed-fail]
                        Err(e) => {
                            tracing::warn!(
                                path = %chunk.file_path,
                                heading = %chunk.heading,
                                error = %e,
                                "embed failed, skipping chunk"
                            );
                        }
                    }
                }
                Ok(result)
            }
        }
    }

    fn chunks_schema(dimension: usize) -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("file_path", DataType::Utf8, false),
            Field::new("heading", DataType::Utf8, false),
            Field::new("content", DataType::Utf8, false),
            Field::new("date", DataType::Utf8, true),
            Field::new("doc_type", DataType::Utf8, true),
            Field::new("tags", DataType::Utf8, true),
            Field::new("project", DataType::Utf8, true),
            Field::new("content_hash", DataType::Utf8, false),
            Field::new("model_name", DataType::Utf8, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(
                    Arc::new(Field::new("item", DataType::Float32, true)),
                    dimension as i32,
                ),
                true,
            ),
        ])
    }

    fn chunks_to_record_batch(
        chunks: &[(Chunk, Vec<f32>, String)],
        schema: &Schema,
        dimension: usize,
        model_name: &str,
    ) -> Result<RecordBatch> {
        use arrow_array::{ArrayRef, Float32Array, StringArray};

        let ids: Vec<String> = chunks
            .iter()
            .map(|(c, _, _)| chunk_id(&c.file_path, &c.heading))
            .collect();
        let file_paths: Vec<&str> = chunks.iter().map(|(c, _, _)| c.file_path.as_str()).collect();
        let headings: Vec<&str> = chunks.iter().map(|(c, _, _)| c.heading.as_str()).collect();
        let contents: Vec<&str> = chunks.iter().map(|(c, _, _)| c.content.as_str()).collect();
        let dates: Vec<Option<&str>> = chunks
            .iter()
            .map(|(c, _, _)| c.frontmatter.date.as_deref())
            .collect();
        let doc_types: Vec<Option<&str>> = chunks
            .iter()
            .map(|(c, _, _)| c.frontmatter.doc_type.as_deref())
            .collect();
        let tags: Vec<Option<String>> = chunks
            .iter()
            .map(|(c, _, _)| c.frontmatter.tags.as_ref().map(serde_json::to_string).transpose())
            .collect::<Result<Vec<_>, _>>()?;
        let tags: Vec<Option<&str>> = tags.iter().map(|t| t.as_deref()).collect();
        let projects: Vec<Option<&str>> = chunks
            .iter()
            .map(|(c, _, _)| c.frontmatter.project.as_deref())
            .collect();
        let content_hashes: Vec<&str> = chunks.iter().map(|(_, _, h)| h.as_str()).collect();
        // r[impl index.embed.model-version]
        let model_names: Vec<&str> = std::iter::repeat_n(model_name, chunks.len()).collect();

        let flat_values: Vec<f32> = chunks.iter().flat_map(|(_, v, _)| v.iter().copied()).collect();
        let values = Float32Array::from(flat_values);
        let vector = arrow_array::FixedSizeListArray::new(
            Arc::new(Field::new("item", DataType::Float32, true)),
            dimension as i32,
            Arc::new(values),
            None,
        );

        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(StringArray::from(ids)),
                Arc::new(StringArray::from(file_paths)),
                Arc::new(StringArray::from(headings)),
                Arc::new(StringArray::from(contents)),
                Arc::new(StringArray::from(dates)),
                Arc::new(StringArray::from(doc_types)),
                Arc::new(StringArray::from(tags)),
                Arc::new(StringArray::from(projects)),
                Arc::new(StringArray::from(content_hashes)),
                Arc::new(StringArray::from(model_names)),
                Arc::new(vector) as ArrayRef,
            ],
        )?;
        Ok(batch)
    }

    fn empty_batch(schema: &Schema, dimension: usize) -> Result<RecordBatch> {
        use arrow_array::{ArrayRef, Float32Array, StringArray};

        let empty_str = StringArray::from(Vec::<String>::new());
        let empty_vec = Float32Array::from(Vec::<f32>::new());
        let vector = arrow_array::FixedSizeListArray::new(
            Arc::new(Field::new("item", DataType::Float32, true)),
            dimension as i32,
            Arc::new(empty_vec),
            None,
        );
        RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(empty_str.clone()),
                Arc::new(empty_str.clone()),
                Arc::new(empty_str.clone()),
                Arc::new(empty_str.clone()),
                Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
                Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
                Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
                Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
                Arc::new(empty_str.clone()),
                Arc::new(empty_str.clone()),
                Arc::new(vector) as ArrayRef,
            ],
        )
        .map_err(Into::into)
    }

    async fn create_fts_index(&self, table: &Table) -> Result<()> {
        table
            .create_index(&["content"], Index::FTS(Default::default()))
            .execute()
            .await?;
        Ok(())
    }

    async fn open_or_create_table(&self, dimension: usize) -> Result<Table> {
        if self.db.table_names().execute().await?.contains(&"chunks".to_string()) {
            Ok(self.db.open_table("chunks").execute().await?)
        } else {
            let schema = Self::chunks_schema(dimension);
            let batch = Self::empty_batch(&schema, dimension)?;
            let batches = RecordBatchIterator::new(
                vec![Ok(batch)].into_iter(),
                Arc::new(schema.clone()),
            );
            self.db
                .create_table("chunks", Box::new(batches))
                .execute()
                .await
        }
        .map_err(Into::into)
    }

    async fn get_stored_hash(&self, table: &Table, file_path: &str) -> Result<Option<String>> {
        let stream = table
            .query()
            .select(Select::columns(&["content_hash"]))
            .only_if(format!("file_path = '{}'", escape_sql_string(file_path)))
            .limit(1)
            .execute()
            .await?;
        let batches: Vec<RecordBatch> = stream.try_collect().await?;
        Ok(batches.first().and_then(|b| {
            b.column_by_name("content_hash").and_then(|col| {
                let arr = arrow_array::cast::as_string_array(col);
                if arr.len() > 0 {
                    Some(arr.value(0).to_string())
                } else {
                    None
                }
            })
        }))
    }

    async fn list_stored_file_paths(&self, table: &Table) -> Result<Vec<String>> {
        let stream = table
            .query()
            .select(Select::columns(&["file_path"]))
            .execute()
            .await?;
        let batches: Vec<RecordBatch> = stream.try_collect().await?;
        let mut paths = std::collections::HashSet::new();
        for batch in batches {
            if let Some(col) = batch.column_by_name("file_path") {
                let arr = arrow_array::cast::as_string_array(col);
                for i in 0..arr.len() {
                    paths.insert(arr.value(i).to_string());
                }
            }
        }
        Ok(paths.into_iter().collect())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::embed::Embedder;
    use crate::embed::MockEmbedder;
    use tempfile::TempDir;

    const DIM: usize = 8;

    async fn setup_indexer() -> (Indexer, TempDir, TempDir) {
        let brain = TempDir::new().unwrap();
        let index = TempDir::new().unwrap();
        let embedder = Arc::new(MockEmbedder::new(DIM, "test-model"));
        let indexer = Indexer::new(
            brain.path().to_path_buf(),
            index.path().to_path_buf(),
            embedder,
        )
        .await
        .unwrap();
        (indexer, brain, index)
    }

    async fn write_md(path: &std::path::Path, content: &str) {
        tokio::fs::create_dir_all(path.parent().unwrap())
            .await
            .unwrap();
        tokio::fs::write(path, content).await.unwrap();
    }

    // r[verify index.full] r[verify index.scan.root] r[verify index.storage.lance]
    #[tokio::test]
    async fn full_reindex_indexes_all_md_files() {
        let (indexer, brain, _) = setup_indexer().await;
        write_md(&brain.path().join("a.md"), "## Section\nContent a.").await;
        write_md(&brain.path().join("b.md"), "## Section\nContent b.").await;

        let stats = indexer.full_reindex().await.unwrap();
        assert_eq!(stats.files_processed, 2);
        assert_eq!(stats.chunks_indexed, 2);
    }

    // r[verify index.scan.md-only]
    #[tokio::test]
    async fn scan_only_md_files() {
        let (indexer, brain, _) = setup_indexer().await;
        write_md(&brain.path().join("a.md"), "## Section\nContent.").await;
        write_md(&brain.path().join("b.txt"), "Not markdown.").await;

        let stats = indexer.full_reindex().await.unwrap();
        assert_eq!(stats.files_processed, 1);
        assert_eq!(stats.chunks_indexed, 1);
    }

    // r[verify index.scan.recursive]
    #[tokio::test]
    async fn scan_recursive() {
        let (indexer, brain, _) = setup_indexer().await;
        write_md(&brain.path().join("root.md"), "## Root\nContent.").await;
        write_md(&brain.path().join("sub/dir/nested.md"), "## Nested\nContent.").await;

        let stats = indexer.full_reindex().await.unwrap();
        assert_eq!(stats.files_processed, 2);
        assert_eq!(stats.chunks_indexed, 2);
    }

    // r[verify index.incremental]
    #[tokio::test]
    async fn incremental_skips_unchanged() {
        let (indexer, brain, _) = setup_indexer().await;
        write_md(&brain.path().join("a.md"), "## Section\nContent.").await;

        let s1 = indexer.full_reindex().await.unwrap();
        assert_eq!(s1.files_processed, 1);
        assert_eq!(s1.chunks_indexed, 1);

        let s2 = indexer.incremental_reindex(&[]).await.unwrap();
        assert_eq!(s2.files_processed, 0);
        assert_eq!(s2.chunks_indexed, 0);
    }

    // r[verify index.upsert]
    #[tokio::test]
    async fn incremental_updates_changed() {
        let (indexer, brain, _) = setup_indexer().await;
        let path = brain.path().join("a.md");
        write_md(&path, "## Section\nOriginal content.").await;

        indexer.full_reindex().await.unwrap();
        write_md(&path, "## Section\nUpdated content.").await;

        let stats = indexer.incremental_reindex(&[]).await.unwrap();
        assert_eq!(stats.files_processed, 1);
        assert_eq!(stats.chunks_indexed, 1);

        let catalog = indexer.catalog().await.unwrap();
        assert_eq!(catalog.len(), 1, "should still be one file after upsert");

        let table = indexer.db.open_table("chunks").execute().await.unwrap();
        let stream = table.query().execute().await.unwrap();
        let batches: Vec<_> = futures::TryStreamExt::try_collect(stream).await.unwrap();
        let content_col = batches[0]
            .column_by_name("content")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        assert!(
            content_col.value(0).contains("Updated content"),
            "index should contain updated content, got: {}",
            content_col.value(0)
        );
    }

    // r[verify index.stale-removal]
    #[tokio::test]
    async fn stale_removal_preserves_other_files() {
        let (indexer, brain, _) = setup_indexer().await;
        write_md(&brain.path().join("keep.md"), "## Keep\nStays.").await;
        write_md(&brain.path().join("remove.md"), "## Remove\nGoes away.").await;

        indexer.full_reindex().await.unwrap();
        let catalog = indexer.catalog().await.unwrap();
        assert_eq!(catalog.len(), 2);

        tokio::fs::remove_file(brain.path().join("remove.md"))
            .await
            .unwrap();
        indexer.incremental_reindex(&[]).await.unwrap();

        let catalog = indexer.catalog().await.unwrap();
        assert_eq!(catalog.len(), 1);
        assert_eq!(catalog[0].file_path, "keep.md");
    }

    // r[verify index.error.embed-fail]
    #[tokio::test]
    async fn embed_failure_skips_chunk() {
        struct FailingEmbedder;
        impl Embedder for FailingEmbedder {
            fn embed(&self, _: &[String]) -> anyhow::Result<Vec<Vec<f32>>> {
                Err(anyhow::anyhow!("embed failed"))
            }
            fn model_name(&self) -> &str {
                "failing"
            }
            fn dimension(&self) -> usize {
                8
            }
        }

        let brain = TempDir::new().unwrap();
        let index = TempDir::new().unwrap();
        write_md(&brain.path().join("a.md"), "## Section\nContent.").await;
        let indexer = Indexer::new(
            brain.path().to_path_buf(),
            index.path().to_path_buf(),
            Arc::new(FailingEmbedder),
        )
        .await
        .unwrap();

        let stats = indexer.full_reindex().await.unwrap();
        assert_eq!(stats.files_processed, 1);
        assert_eq!(stats.chunks_indexed, 0);
    }

    #[tokio::test]
    async fn batch_fail_falls_back_to_per_chunk() {
        use crate::embed::BatchFailEmbedder;

        let brain = TempDir::new().unwrap();
        let index = TempDir::new().unwrap();
        write_md(
            &brain.path().join("multi.md"),
            "## One\nContent one.\n\n## Two\nContent two.",
        )
        .await;
        let embedder = Arc::new(BatchFailEmbedder::new(DIM, "batch-fail-model"));
        let indexer = Indexer::new(
            brain.path().to_path_buf(),
            index.path().to_path_buf(),
            embedder,
        )
        .await
        .unwrap();

        let stats = indexer.full_reindex().await.unwrap();
        assert_eq!(stats.files_processed, 1);
        assert_eq!(stats.chunks_indexed, 2, "both chunks should succeed via per-chunk fallback");
    }

    #[tokio::test]
    async fn catalog_lists_documents() {
        let (indexer, brain, _) = setup_indexer().await;
        write_md(
            &brain.path().join("doc.md"),
            r#"---
date: 2024-01-15
type: note
tags: [a, b]
project: my-project
---
## Section
Content"#,
        )
        .await;

        indexer.full_reindex().await.unwrap();
        let catalog = indexer.catalog().await.unwrap();
        assert_eq!(catalog.len(), 1);
        assert_eq!(catalog[0].file_path, "doc.md");
        assert_eq!(catalog[0].date.as_deref(), Some("2024-01-15"));
        assert_eq!(catalog[0].doc_type.as_deref(), Some("note"));
        assert_eq!(catalog[0].tags.as_ref().unwrap(), &["a", "b"]);
        assert_eq!(catalog[0].project.as_deref(), Some("my-project"));
    }

    #[tokio::test]
    async fn model_name_returns_embedder_model() {
        let (indexer, _, _) = setup_indexer().await;
        assert_eq!(indexer.model_name(), "test-model");
    }

    // r[verify index.startup-reindex] r[verify index.storage.dir-config]
    #[tokio::test]
    async fn startup_incremental_reindex_catches_new_files() {
        let (indexer, brain, _) = setup_indexer().await;
        write_md(&brain.path().join("a.md"), "## Hello\nWorld.").await;

        let stats = indexer.incremental_reindex(&[]).await.unwrap();
        assert_eq!(stats.files_processed, 1);
        assert_eq!(stats.chunks_indexed, 1);

        let catalog = indexer.catalog().await.unwrap();
        assert_eq!(catalog.len(), 1);
        assert_eq!(catalog[0].file_path, "a.md");
    }

    #[tokio::test]
    async fn incremental_reindex_with_specific_paths() {
        let (indexer, brain, _) = setup_indexer().await;
        write_md(&brain.path().join("a.md"), "## A\nContent a.").await;
        write_md(&brain.path().join("b.md"), "## B\nContent b.").await;

        indexer.full_reindex().await.unwrap();

        write_md(&brain.path().join("a.md"), "## A\nUpdated a.").await;

        let stats = indexer
            .incremental_reindex(&[brain.path().join("a.md")])
            .await
            .unwrap();
        assert_eq!(stats.files_processed, 1);
        assert_eq!(stats.chunks_indexed, 1);
    }

    // r[verify index.watch]
    #[tokio::test]
    async fn watcher_reindexes_on_file_change() {
        let (indexer, brain, _) = setup_indexer().await;
        indexer.full_reindex().await.unwrap();
        indexer.spawn_watcher().unwrap();

        write_md(&brain.path().join("new.md"), "## New\nNew content.").await;

        tokio::time::sleep(std::time::Duration::from_secs(4)).await;

        let catalog = indexer.catalog().await.unwrap();
        assert_eq!(catalog.len(), 1);
        assert_eq!(catalog[0].file_path, "new.md");
    }
}
