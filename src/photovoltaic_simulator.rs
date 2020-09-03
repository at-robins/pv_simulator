//! The `photovoltaic_simulator` module allows simulation of photovoltaic power output.
extern crate rand;

use amiquip::{Connection, ConsumerMessage, ConsumerOptions, QueueDeclareOptions};
use chrono::{DateTime, NaiveTime, Timelike, Utc};
use rand::{Rng, thread_rng};
use serde::{Deserialize, Serialize};
use std::fs::{create_dir_all, File};
use std::path::Path;
use super::meter::{BrokerMessage, METER_ROUTING_KEY};
use super::pv_error::PvError;

/// A `PvSimulator` that mimics power output of a photovoltaic system.
#[derive(Debug, PartialEq, Clone)]
pub struct PvSimulator {
    broker_url: String,
    records: Vec<Record>,
}

impl PvSimulator {
    /// Creates a new `PvSimulator` processing power consumption messages recieved from the broker
    /// and simulating power output values in Watt depending on the time of day.
    ///
    /// # Parameters
    ///
    /// * `broker_url` - the url of the broker
    pub fn new<U: Into<String>>(broker_url: U) -> Self {
        PvSimulator{
            broker_url: broker_url.into(),
            records: Vec::new()
        }
    }

    /// Listens for messages available from the broker, processes them and adds them to the
    /// record file. This process ends once a simulation-end-message was recieved.
    /// Fails if the messaging process fails and returns the according error.
    pub fn listen_to_broker(&mut self) -> Result<(), PvError> {
        // Setup a consumer and listen to all incomming messages until the simulation ends.
        let mut connection = Connection::insecure_open(&self.broker_url)?;
        let channel = connection.open_channel(None)?;
        let queue = channel.queue_declare(METER_ROUTING_KEY, QueueDeclareOptions::default())?;
        let consumer = queue.consume(ConsumerOptions::default())?;
        for message in consumer.receiver().iter() {
            match message {
                ConsumerMessage::Delivery(delivery) => {
                    let message: BrokerMessage = serde_json::from_slice(&delivery.body)?;
                    consumer.ack(delivery)?;
                    if message.is_simulation_end() {
                        // Cancel the consumer if the simulation ended.
                        consumer.cancel()?;
                    } else {
                        // If the simulation is ongoing add the message to the records.
                        let record = self.message_to_record(message)?;
                        self.records.push(record);
                    }
                },
                // The consumer is cancelled once the simulation ended.
                ConsumerMessage::ClientCancelled => break,
                other => return Err(
                    PvError::InternalError(format!("Consumer did not expect: {:?}", other))
                ),
            }
        }
        connection.close()?;
        Ok(())
    }

    /// Writes all observed `Record`s to the specified file.
    /// Fails if the file or its parent directory cannot be created.
    ///
    /// # Parameters
    ///
    /// * `path` - the path to the output file
    pub fn write_records_to_file<P: AsRef<Path>>(&self, path: P) -> Result<(), PvError> {
        // Make sure there is a last path component that can be written to.
        let parent_directory = path.as_ref()
            .parent()
            .ok_or(PvError::InternalError(
                format!("{:?} does not point to a file.", path.as_ref())
            ))?;
        // Create parent directories.
        create_dir_all(parent_directory)?;
        // Default writing options are fine for file creation.
        let writer = File::create(path)?;
        serde_json::to_writer(writer, &self.records)?;
        Ok(())
    }

    /// Converts a message from the broker to a record for data output.
    /// Fails if the message contains invalid / empty fields.
    ///
    /// # Parameters
    ///
    /// * `message` - the message from the broker
    fn message_to_record(&self, message: BrokerMessage) -> Result<Record, PvError> {
        if let Some(consumption) = message.power_consumption() {
            if let Some(time) = message.time_stamp() {
                Ok(Record::new(time, consumption, pv_simulation_function(time.time())))
            } else {
                Err(PvError::InternalError(
                    format!("No time stamp was specified for message: {:?}", message)
                ))
            }
        } else {
            Err(PvError::InternalError(
                format!("No power consumption was specified for message: {:?}", message)
            ))
        }
    }
}

/// Simulates the power output of a photovoltaic component in watt by rough approximation with a
/// Kumaraswamy distribution.
///
/// # Parameters
///
/// * `time_of_day` - the time of day in nanosecond precision
fn pv_simulation_function(time_of_day: NaiveTime) -> f64 {
    let time_of_day_in_h = normalised_time_of_day(time_of_day);
    // Dusk and dawn in hours from midnight.
    // These values should be supplied by some external source
    // but are defined in this function for briefty.
    let dusk = 21.0;
    let dawn = 5.0;
    if time_of_day_in_h > dawn && time_of_day_in_h < dusk {
        // Scale the daytime to an interval from 0 to 1, where the
        // Kumaraswamy distribution is defined.
        let x = (time_of_day_in_h - dawn) / (dusk - dawn);
        // Scale the output to the expected power in watt.
        let scaling = 1650.0;
        // The parameters a and b were roughly approximated according to
        // the diagram supplied in the exercise description.
        let simulated_output = kumaraswamy_pdf(2.8, 3.3, x) * scaling;
        // Add some random noise to the simulated data.
        let jitter = thread_rng().gen_range(0.99, 1.01);
        simulated_output * jitter
    } else {
        // Return no power output while the sun is not out.
        0.0
    }
}

/// The probability density function of the Kumaraswamy distribution.
///
/// # Parameters
///
/// * `a` - parameter a of the Kumaraswamy distribution
/// * `b` - parameter b of the Kumaraswamy distribution
/// * `x` - the probability input
fn kumaraswamy_pdf(a: f64, b: f64, x: f64) -> f64 {
    a * b * x.powf(a - 1.0) * (1.0 - x.powf(a)).powf(b - 1.0)
}

/// Normalises a `NativeTime` to a single floating point value in hours.
///
/// # Parameters
///
/// * `time` - the time to normalise
fn normalised_time_of_day(time: NaiveTime) -> f64 {
    time.hour() as f64
        + time.minute() as f64 / 60.0
        + time.second() as f64 / 3_600.0
        + time.nanosecond() as f64 / 3_600_000_000_000.0
}

#[derive(Debug, PartialEq, Clone, Copy, Serialize, Deserialize)]
pub struct Record {
    time_stamp: DateTime<Utc>,
    meter_power_consumption: f64,
    pv_power_output: f64,
    total_power_output: f64,
}

impl Record {
    /// Creates a new `Record` summarising the simulation data.
    ///
    /// # Parameters
    ///
    /// * `time_stamp` - the time stamp of the simulation data point
    /// * `meter_power_consumption` - the power consumption as simulated by
    /// the corresponding `Meter`
    /// * `pv_power_output` - the power output as simulated by the corresponding
    /// photovoltaic component
    fn new(time_stamp: DateTime<Utc>, meter_power_consumption: f64, pv_power_output: f64) -> Self {
        Record{
            time_stamp,
            meter_power_consumption,
            pv_power_output,
            // Power consumption and output per definition have different signs,
            // so addition of both values as specified in the exercise"s description
            // results in subtraction.
            total_power_output: pv_power_output - meter_power_consumption,
        }
    }

    // Returns the time stamp of this `Record`.
    pub fn _time_stamp(&self) -> DateTime<Utc> {
        self.time_stamp
    }

    // Returns the power consumption of this `Record` indicated by the corrsponding `Meter`.
    pub fn _power_consumption(&self) -> f64 {
        self.meter_power_consumption
    }

    // Returns the power output of this `Record` indicated by the corrsponding `PvSimulator`.
    pub fn _power_output(&self) -> f64 {
        self.pv_power_output
    }

    // Returns the total power output of this `Record` indicated by the corrsponding `Meter`
    // and `PvSimulator`.
    pub fn _total_power_output(&self) -> f64 {
        self.total_power_output
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::float_compare_non_exact;

    #[test]
    /// Tests if the function `normalised_time_of_day` performes a correct normalisation to hours.
    fn test_normalised_time_of_day() {
        let time = NaiveTime::from_hms_nano(20, 15, 36, 360_000_000);
        let expected = 20.2601;
        assert!(float_compare_non_exact(expected, normalised_time_of_day(time)));
    }

    #[test]
    /// Tests if the function `pv_simulation_function` produces output according to the diagram
    /// displayed in the exercise's description. Indirectly also tests the function
    /// `kumaraswamy_pdf`.
    fn test_pv_simulation_function() {
        // Test are performed according to the diagram displayed in the exercise's description.

        // No output before dawn.
        {
            let time = NaiveTime::from_hms(0, 0, 0);
            let simulated_output = pv_simulation_function(time);
            assert_eq!(simulated_output, 0.0);
        }
        // Output starting at dawn.
        {
            let time = NaiveTime::from_hms(5, 0, 0);
            let simulated_output = pv_simulation_function(time);
            assert!(float_compare_non_exact(simulated_output, 0.0));
        }
        // Increasing output after dawn.
        {
            let time = NaiveTime::from_hms(10, 0, 0);
            let simulated_output = pv_simulation_function(time);
            assert!(float_compare_pv_power_output(simulated_output, 1750.0));
        }
        // Maximum output around 2pm.
        {
            let time = NaiveTime::from_hms(14, 0, 0);
            let simulated_output = pv_simulation_function(time);
            assert!(float_compare_pv_power_output(simulated_output, 3300.0));
        }
        // Decreasing output after 2 pm.
        {
            let time = NaiveTime::from_hms(18, 0, 0);
            let simulated_output = pv_simulation_function(time);
            assert!(float_compare_pv_power_output(simulated_output, 1750.0));
        }
        // Output stopping at dusk.
        {
            let time = NaiveTime::from_hms(21, 0, 0);
            let simulated_output = pv_simulation_function(time);
            assert!(float_compare_non_exact(simulated_output, 0.0));
        }
        // No output after dusk.
        {
            let time = NaiveTime::from_hms(22, 30, 0);
            let simulated_output = pv_simulation_function(time);
            assert_eq!(simulated_output, 0.0);
        }
    }

    /// Compares pv simulation and expected value allowing for a small relative variance.
    ///
    /// # Parameters
    ///
    /// * `simulated` - the simulation result
    /// * `expected` - the reference obtained from the diagram in the exercise's description
    fn float_compare_pv_power_output(simulated: f64, expected: f64) -> bool {
        // Allow 10% variance.
        (1.0 - (simulated / expected)).abs() <= 0.1
    }

    #[test]
    /// Tests if the function `float_compare_pv_power_output` compares power output values
    /// in a similar range correctly.
    fn test_float_compare_pv_power_output() {
        // Test similar values.
        {
            let a = 3400.0;
            let b = 3200.0;
            assert!(a != b);
            assert!(float_compare_pv_power_output(a, b));
        }
        // Test different values.
        {
            let a = 1750.0;
            let b = 2500.0;
            assert!(a != b);
            assert!(!float_compare_pv_power_output(a, b));
        }
    }
}
