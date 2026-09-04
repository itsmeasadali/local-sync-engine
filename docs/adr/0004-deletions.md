# ADR 0004: Versioned record tombstones

Status: accepted.

A delete advances a record tombstone without physically erasing fields. Fields at or below the tombstone are invisible, preventing stale mutations from resurrecting a record. A strictly newer field mutation intentionally resurrects only those newer fields. Tombstone retention is indefinite in v1 because safe garbage collection requires knowledge of all replica watermarks.
