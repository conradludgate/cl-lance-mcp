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
Search MUST combine vector similarity with full-text keyword matching so that
exact identifiers (Jira tickets, instance IDs, names) surface alongside
semantically similar results. The server MUST create an FTS index on the content
column and use LanceDB's native hybrid query support.

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
