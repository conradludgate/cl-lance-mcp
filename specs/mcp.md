# MCP Server

The MCP server exposes the brain's RAG capabilities to Cursor via three tools
over stdio transport. It is a single Rust binary with no external runtime
dependencies.

## Transport

r[mcp.transport.stdio]
The server MUST use stdio transport (stdin/stdout) so Cursor can launch it
directly as a subprocess.

## Tools

r[mcp.tool.search]
The server MUST expose a `search` tool that accepts a `query` string and
optional `top_k` integer parameter.

r[mcp.tool.search.top-k-param]
The `search` tool's `top_k` parameter MUST be optional.

r[mcp.tool.reindex]
The server MUST expose a `reindex` tool that accepts an optional `path` string
parameter to scope reindexing to a specific file or directory.

r[mcp.tool.reindex.path-param]
The `reindex` tool's `path` parameter MUST be optional. When omitted, all files
under the brain root are reindexed.

r[mcp.tool.reindex.path-invalid]
When `reindex` is called with a path that does not exist or is outside the brain
root, the server MUST return an error.

r[mcp.tool.catalog]
The server MUST expose a `catalog` tool with no required parameters.

r[mcp.tool.catalog.list]
The `catalog` tool MUST return all indexed documents with file path, date, type,
tags, and project metadata.
