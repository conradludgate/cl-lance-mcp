# Search

Search embeds a query using the same model as indexing, finds the nearest
vectors in LanceDB, and returns ranked chunks with metadata.

## Query Flow

r[search.embed-query]
The query string MUST be embedded using the same model and parameters used for
indexing.

r[search.similarity]
Search MUST return the top-k nearest vectors by cosine similarity from LanceDB.

r[search.default-top-k]
When `top_k` is not specified, search MUST default to 10 results.

## Hybrid Search

r[search.hybrid]
Search SHOULD support hybrid mode combining vector similarity with full-text
keyword matching for exact identifiers (Jira tickets, instance IDs, names). If
the Rust LanceDB SDK does not support native hybrid queries, the server MUST
implement two-pass search (vector + FTS) and merge results in application code.

## Results

r[search.result.text]
Each search result MUST include the chunk text content.

r[search.result.meta]
Each search result MUST include file path, heading, and frontmatter metadata.

## Error Handling

r[search.error.no-index]
If the index directory does not exist or is empty, search MUST return an error
indicating reindexing is required.

r[search.error.empty-query]
When the query string is empty or only whitespace, the server MUST return an
error.

r[search.error.model-mismatch]
When the configured embedding model differs from the model recorded in the
index, search MUST return an error indicating a full reindex is required.
