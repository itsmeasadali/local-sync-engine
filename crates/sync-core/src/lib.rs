#![forbid(unsafe_code)]
//! Transport-independent hybrid-clock, validation, and deterministic merge logic.

mod hlc;
mod merge;
mod validation;

pub use hlc::{ClockError, HybridLogicalClock};
pub use merge::{FieldState, RecordState};
pub use validation::{validate_mutation, ValidationError, ValidationLimits};
