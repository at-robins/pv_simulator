//! The `simulated_time` module allows simulation of time in fixed intervalls.
extern crate chrono;

use chrono::{DateTime, Duration, Utc};

/// `SimulatedDateTime` simulates a time point and its flow in fixed inervalls.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct SimulatedDateTime {
    starting_time: DateTime<Utc>,
    current_time: DateTime<Utc>,
    stride: Duration,
    max_simulated_time: Duration,
}

impl SimulatedDateTime {
    /// Creates a new `SimulatedDateTime` that increases strictly monoton on every call.
    ///
    /// # Parameters
    ///
    /// * `stride` - the `Duration` that is passing between two subsequent calls
    /// * `max_simulated_time` - the maximum length of the simulation
    ///
    /// # Panics
    ///
    /// If the `stride` is smaller or equal to zero.
    pub fn new(stride: Duration, max_simulated_time: Duration) -> Self {
        if stride <= Duration::zero() {
            panic!("The simulated time must increase strictly monoton!");
        }
        let starting_time = Utc::now();
        SimulatedDateTime {
            starting_time,
            current_time: starting_time,
            stride,
            max_simulated_time,
        }
    }

    /// Increments the `SimulatedDateTime` by its specified stride and returns the
    /// new simulated `DateTime` if the maximum simulation length is not exceeded.
    pub fn current_date_time(&mut self) -> Option<DateTime<Utc>> {
        if self.current_time - self.starting_time > self.max_simulated_time {
            None
        } else {
            let old_time = self.current_time;
            self.current_time = self.current_time + self.stride;
            Some(old_time)
        }
    }
}

impl Iterator for SimulatedDateTime {
    type Item = DateTime<Utc>;

    fn next(&mut self) -> Option<Self::Item> {
        self.current_date_time()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    /// Tests if the function `current_date_time` will correctly increase the simulated time
    /// and stop returning after it exceeded its maximum simulation length.
    fn test_current_date_time() {
        let stride = Duration::seconds(1);
        let mut simulated_time = SimulatedDateTime::new(stride, Duration::seconds(5000));
        let mut expected_date_time = simulated_time.current_time;
        // Simulate the initial time point.
        assert_eq!(Some(expected_date_time), simulated_time.current_date_time());
        // Simulate all further possible time points.
        for _ in 0..5000 {
            // AddAssign is not implemented for DateTime.
            expected_date_time = expected_date_time + stride;
            assert_eq!(Some(expected_date_time), simulated_time.current_date_time());
        }
        // The maximum simulation length was reached.
        assert_eq!(None, simulated_time.current_date_time());
    }

    #[test]
    /// Tests if the `Iterator` implementation returns the expected values.
    fn test_iterator() {
        let stride = Duration::seconds(5);
        let simulated_time = SimulatedDateTime::new(stride, Duration::seconds(1000));
        let start_date_time = simulated_time.starting_time;
        let expected_dates: Vec<DateTime<Utc>> = (0..201)
            .map(|i| start_date_time + Duration::seconds(i * 5))
            .collect();
        // The maximum simulation length was reached.
        assert_eq!(
            expected_dates,
            simulated_time.collect::<Vec<DateTime<Utc>>>()
        );
    }

    #[test]
    #[should_panic]
    /// Tests if the function `new` will correctly panic on zero strides.
    fn test_panic_new_zero() {
        SimulatedDateTime::new(Duration::zero(), Duration::seconds(1));
    }

    #[test]
    #[should_panic]
    /// Tests if the function `new` will correctly panic on negative strides.
    fn test_panic_new_negative() {
        SimulatedDateTime::new(Duration::minutes(-12), Duration::seconds(1));
    }
}
