# ADR 0001: Immutable operation log

Status: accepted.

The server stores validated mutations as immutable JSON alongside a workspace-local sequence. This makes retry deduplication, incremental pull, and audit possible without coupling the protocol to a server materialization schema. The cost is unbounded log growth; compaction and snapshots require a future compatible protocol extension.
