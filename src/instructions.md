Personal knowledge base ("second brain") semantic search over a collection of markdown files.

## Tools

### search
Hybrid search (semantic + full-text keyword). Works for both natural language queries
and exact identifiers (ticket IDs, error codes, names). Returns ranked chunks with
file path, heading, date, type, tags, and project metadata.

### catalog
Lists all indexed documents with metadata. Use to browse what's available or find
documents by type/date/project without a search query.

### reindex
Rebuilds the search index. Call after creating or modifying brain content to keep
search results current.

## Tips
- Search results return chunks, not full files. Use the file_path to read the full
  document if a chunk looks relevant.
- Results include a score — higher is better (relevance-based hybrid ranking).
- Prefer search over grep/file scanning when looking for prior context on a topic.
