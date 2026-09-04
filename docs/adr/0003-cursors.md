# ADR 0003: Workspace sequence cursors

Status: accepted.

Every accepted mutation gets a sequence from a counter locked per workspace. A pull page contains rows strictly after the input cursor and its cursor is the last row actually returned (or the input cursor when empty). `limit + 1` determines `has_more`. Concurrent inserts always have larger sequences, so pagination neither skips nor invents rows. Clients commit page application and cursor advancement atomically.
