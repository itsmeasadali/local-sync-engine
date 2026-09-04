use sync_protocol::HlcTimestamp;
use thiserror::Error;

#[derive(Debug, Error, Eq, PartialEq)]
pub enum ClockError {
    #[error("hybrid logical clock counter overflowed")]
    LogicalOverflow,
}

/// A persistent HLC. Callers must save `last()` in the same transaction as a local mutation.
#[derive(Clone, Debug)]
pub struct HybridLogicalClock {
    last: HlcTimestamp,
}

impl HybridLogicalClock {
    #[must_use]
    pub const fn new(last: HlcTimestamp) -> Self {
        Self { last }
    }

    #[must_use]
    pub const fn last(&self) -> HlcTimestamp {
        self.last
    }

    /// Advances the clock for a local event.
    ///
    /// # Errors
    ///
    /// Returns [`ClockError::LogicalOverflow`] when the logical component
    /// cannot be incremented.
    pub fn tick(&mut self, wall_ms: u64) -> Result<HlcTimestamp, ClockError> {
        self.last = if wall_ms > self.last.physical_ms {
            HlcTimestamp {
                physical_ms: wall_ms,
                logical: 0,
            }
        } else {
            HlcTimestamp {
                physical_ms: self.last.physical_ms,
                logical: self
                    .last
                    .logical
                    .checked_add(1)
                    .ok_or(ClockError::LogicalOverflow)?,
            }
        };
        Ok(self.last)
    }

    /// Incorporates a remote timestamp and advances the clock.
    ///
    /// # Errors
    ///
    /// Returns [`ClockError::LogicalOverflow`] when the logical component
    /// cannot be incremented.
    pub fn observe(
        &mut self,
        remote: HlcTimestamp,
        wall_ms: u64,
    ) -> Result<HlcTimestamp, ClockError> {
        let physical_ms = wall_ms.max(self.last.physical_ms).max(remote.physical_ms);
        let logical = match (
            physical_ms == self.last.physical_ms,
            physical_ms == remote.physical_ms,
        ) {
            (true, true) => self.last.logical.max(remote.logical).checked_add(1),
            (true, false) => self.last.logical.checked_add(1),
            (false, true) => remote.logical.checked_add(1),
            (false, false) => Some(0),
        }
        .ok_or(ClockError::LogicalOverflow)?;
        self.last = HlcTimestamp {
            physical_ms,
            logical,
        };
        Ok(self.last)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clock_never_moves_backwards() {
        let mut clock = HybridLogicalClock::new(HlcTimestamp::default());
        assert!(clock.tick(100).unwrap() < clock.tick(100).unwrap());
        assert!(clock.tick(100).unwrap() < clock.tick(99).unwrap());
        assert!(
            clock.last()
                < clock
                    .observe(
                        HlcTimestamp {
                            physical_ms: 200,
                            logical: 5
                        },
                        150
                    )
                    .unwrap()
        );
    }
}
