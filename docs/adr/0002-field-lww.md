# ADR 0002: Field-level LWW registers

Status: accepted.

Each changed field is a max-register ordered by `(HLC physical, HLC logical, actor ID)`. Actor ID supplies a deterministic tie-breaker, so joins are idempotent, commutative, and associative. Different fields merge independently.

LWW can lose a concurrent same-field edit and a badly advanced clock can dominate until time catches up. The server rejects timestamps too far in the future but never changes accepted timestamps. This model is unsuitable for collaborative text, counters, or sequence editing.
