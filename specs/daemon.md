# Leader-Follower Coordination

When multiple MCP server instances share the same index directory, only one
instance (the leader) owns the file watcher and reindex pipeline. Others
(followers) forward reindex requests to the leader over a Unix socket.

## Leader Election

r[daemon.lock]
On startup, the server MUST attempt to acquire an exclusive `flock` on
`$INDEX_DIR/brain.lock`. If the lock is acquired, the instance is the leader.
If the lock cannot be acquired, the instance is a follower.

r[daemon.leader.watcher]
Only the leader MUST run the file watcher.

r[daemon.leader.startup-reindex]
Only the leader MUST perform the startup incremental reindex.

## Reindex Forwarding

r[daemon.leader.socket]
The leader MUST listen on `$INDEX_DIR/brain.sock` for reindex requests from
followers. The protocol is a single newline-delimited JSON exchange per
connection: the follower sends a request, the leader executes it and sends
back a response.

r[daemon.follower.reindex-forward]
When a follower receives a reindex tool call, it MUST forward the request
to the leader over the Unix socket rather than writing to the index directly.

r[daemon.follower.reindex-unavailable]
If the follower cannot connect to the leader's socket, it MUST return an
error indicating that the leader instance is unavailable.

## Cleanup

r[daemon.socket.cleanup]
The leader MUST remove `brain.sock` before binding (to handle stale leftovers
from crashes) and SHOULD remove it on clean shutdown.
