# Indexing

The indexing pipeline scans markdown files, chunks them, embeds the chunks, and
stores vectors in LanceDB. It supports both full and incremental reindexing.

## Scanning

r[index.scan.root]
The scanner MUST use a configurable brain root directory as the base for finding
files.

r[index.scan.recursive]
The scanner MUST recursively traverse subdirectories under the brain root.

r[index.scan.md-only]
The scanner MUST only index `.md` files.

## Reindex Modes

r[index.full]
A full reindex MUST scan all `.md` files under the brain root, chunk them,
embed each chunk, and write all vectors to LanceDB.

r[index.incremental]
An incremental reindex MUST re-embed only files whose content has changed,
detected by content hash or modification time.

r[index.upsert]
Re-embedding a file MUST replace its previous chunks in the index, not
duplicate them.

r[index.stale-removal]
After reindexing, chunks from deleted or renamed files MUST be removed from the
index.

## Embedding

r[index.embed.in-process]
Embedding MUST use an in-process ONNX model via `fastembed`. No external
process or network calls are permitted.

r[index.embed.model-version]
The index MUST record which embedding model produced it. If the configured model
differs from the stored model, the server MUST reject queries and require a full
reindex.

## Storage

r[index.storage.lance]
Vectors and metadata MUST be stored in LanceDB Arrow files.

r[index.storage.dir-config]
The index directory path MUST be configurable. The default MUST be
`.brain-index/` relative to the brain root.

## Automatic Reindexing

r[index.startup-reindex]
On startup, the server MUST perform an incremental reindex before accepting tool
calls, so the index reflects any files changed while the server was not running.

r[index.watch]
The server MUST watch the brain root for filesystem changes and trigger an
incremental reindex after a 2-second debounce period following the last detected
change.

## Error Handling

r[index.error.embed-fail]
If embedding a chunk fails, the server MUST log the error and skip that chunk
without aborting the entire reindex.
