# brain-mcp

MCP server for semantic search over a directory of markdown files.
Runs as a stdio transport, watches for file changes, and supports hybrid
vector + full-text search.

## Install

Requires Rust toolchain ([rustup.rs](https://rustup.rs)).

```bash
cargo install --git https://github.com/conradludgate/cl-lance-mcp
```

## Configuration

| Env var | Default | Description |
|---------|---------|-------------|
| `BRAIN_ROOT` | Current directory | Root directory of markdown files |
| `BRAIN_INDEX_DIR` | `$BRAIN_ROOT/.brain-index` | Where the LanceDB index is stored |

The index directory should be gitignored.

## Usage with Cursor

Add to `.cursor/mcp.json`:

```json
{
  "mcpServers": {
    "brain": {
      "command": "cl-lance-mcp",
      "env": {
        "BRAIN_ROOT": "/path/to/your/brain"
      }
    }
  }
}
```

## Usage with Claude Code

```bash
claude mcp add brain -- cl-lance-mcp
```

Or with explicit brain root:

```bash
claude mcp add brain -- env BRAIN_ROOT=/path/to/your/brain cl-lance-mcp
```

## Usage with other MCP clients

The server uses stdio transport. Launch the binary with `BRAIN_ROOT` set:

```bash
BRAIN_ROOT=/path/to/your/brain cl-lance-mcp
```

## Tools

| Tool | Description |
|------|-------------|
| `search` | Hybrid semantic + keyword search. Accepts a query and optional `top_k`. |
| `catalog` | Lists all indexed documents with metadata. |
| `reindex` | Triggers a reindex. Optionally scoped to a path. |

## How it works

- Splits markdown files into chunks at `##`/`###` headings
- Embeds chunks using FastEmbed (ONNX, in-process)
- Stores vectors + metadata in LanceDB with a full-text index
- On startup, performs an incremental reindex to catch offline changes
- Watches the brain root for file changes and reindexes automatically
