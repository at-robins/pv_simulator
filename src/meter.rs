//! The `meter` module allows simulation of power consumption.
extern crate rand;

use amiquip::{Connection, Exchange, Publish};
use chrono::{DateTime, Utc};
use rand::{Rng, thread_rng};
use serde::{Deserialize, Serialize};
use super::PvError;
use super::SimulatedDateTime;

/// The routing key for the RabbitMQ message broker.
pub const METER_ROUTING_KEY: &str = "meter_queue";

/// A `Meter` that mimics power consumption by producing continuous randomly distributed
/// power values.
#[derive(Debug, PartialEq, Clone)]
pub struct Meter {
    consumption_bound: f64,
    broker_url: String,
}

impl Meter {
    /// Creates a new `Meter` sampling random power consumption values in Watt.
    /// If zero is specified as upper bound, only zero values will be sampled.
    /// Fails, if the `consumption_bound` is not a positive finite number.
    ///
    /// # Parameters
    ///
    /// * `consumption_bound` - the exclusive upper bound of power consumption
    /// * `broker_url` - the url of the broker
    pub fn new<U: Into<String>>(consumption_bound: f64, broker_url: U) -> Result<Self, PvError> {
        if consumption_bound.is_finite() && consumption_bound.is_sign_positive() {
            Ok(Meter{
                consumption_bound,
                broker_url: broker_url.into()
            })
        } else {
            Err(PvError::InternalError(
                format!("{} is not a positive finite number.", consumption_bound)
            ))
        }
    }

    /// Samples a random value from the `Meter`.
    pub fn sample(&self) -> f64 {
        if self.consumption_bound == 0.0 {
            // If the upper bound was specified to be zero, there is no need to sample.
            0.0
        } else {
            // Samples from a unfiform distrubution. This fullfills the requirement of creating
            // continuous randomly distributed values as stated in the exercise's specifications.
            thread_rng().gen_range(0.0, self.consumption_bound)
        }
    }

    /// Publishes the specified message to the broker.
    ///
    /// * `message` - the message to publish
    fn publish_to_broker(&self, message: BrokerMessage) -> Result<(), PvError>{
        // Open an insecure connection to omit OpenSSL as dependency for
        // this example.
        let mut connection = Connection::insecure_open(&self.broker_url)?;
        let channel = connection.open_channel(None)?;
        let exchange = Exchange::direct(&channel);
        // JSON, as widely used format, is exploited for serialisation to be agnostic
        // to the other parts of the system.
        // WARNING: serde_json does currently not support native bit precision floating point
        // serialisation. This is ignored here for the sake of simplicity.
        let serialised_message = serde_json::to_vec(&message)?;
        exchange.publish(Publish::new(&serialised_message, METER_ROUTING_KEY))?;
        channel.close()?;
        Ok(())
    }

    /// Samples a random value from the `Meter`, publishes it to the broker and returns it.
    ///
    /// * `sampling_time` - the time point of sampling
    pub fn publish_sample(&self, sampling_time: DateTime<Utc>) -> Result<f64, PvError> {
        let sample = self.sample();
        let message = BrokerMessage::new(sample, sampling_time)?;
        self.publish_to_broker(message)?;
        Ok(sample)
    }

    /// Notifies clients that the simulation has finished.
    pub fn publish_simulation_end(&self) -> Result<(), PvError> {
        self.publish_to_broker(BrokerMessage::simulation_end_message())
    }
}

/// A `BrokerMessage` contains all information a `Meter needs to publish
/// to a corresponding broker.
#[derive(Debug, PartialEq, Clone, Copy, Serialize, Deserialize)]
pub struct BrokerMessage {
    power_consumption: Option<f64>,
    time_stamp: Option<DateTime<Utc>>,
}

impl BrokerMessage {
    /// Creates a new `BrokerMessage`.
    /// Fails, if the `power_consumption` is not a positive finite number.
    /// A `None` as power consumption indicates an end of the simulation.
    ///
    /// # Parameters
    ///
    /// * `power_consumption` - the power consumption to be sent to the broker
    /// * `time_stamp` - the sampling time point
    pub fn new(power_consumption: f64, time_stamp: DateTime<Utc>) -> Result<Self, PvError> {
        if power_consumption.is_finite() && power_consumption.is_sign_positive() {
            Ok(BrokerMessage{
                power_consumption: Some(power_consumption),
                time_stamp: Some(time_stamp),
            })
        } else {
            Err(PvError::InternalError(
                format!("{} is not a positive finite number.", power_consumption)
            ))
        }
    }

    pub fn simulation_end_message() -> Self {
        BrokerMessage{
            power_consumption: None,
            time_stamp: None,
        }
    }

    /// Checks if the message indicates the end of the simulation.
    pub fn is_simulation_end(&self) -> bool {
        self.power_consumption.is_none()
    }

    /// Returns the time_stamp specified by this message, if any.
    pub fn time_stamp(&self) -> Option<DateTime<Utc>> {
        self.time_stamp
    }

    /// Returns the power consumption specified by this message, if any.
    pub fn power_consumption(&self) -> Option<f64> {
        self.power_consumption
    }
}

#[cfg(test)]
mod tests {
    use amiquip::{Connection, ConsumerMessage, ConsumerOptions, QueueDeclareOptions};
    use serial_test::serial;
    use super::*;
    use super::super::float_compare_non_exact;

    #[test]
    /// Tests if the function `new` of the `Meter` struct only creates valid `Meter`s.
    fn test_meter_new() {
        // General testing.
        {
            let bound = 1000.0;
            let url = "Test";
            let meter = Meter::new(bound, url);
            assert!(meter.is_ok());
            let meter = meter.unwrap();
            assert_eq!(meter.consumption_bound, bound);
            assert_eq!(meter.broker_url, url);
        }
        // Specific testing.
        assert!(Meter::new(0.0, "").is_ok());
        assert!(Meter::new(-10.0, "").is_err());
        assert!(Meter::new(f64::INFINITY, "").is_err());
        assert!(Meter::new(f64::NEG_INFINITY, "").is_err());
        assert!(Meter::new(f64::NAN, "").is_err());
    }

    #[test]
    /// Tests if the function `sample` of the `Meter` struct does not exceed the upper bound.
    fn test_meter_sample() {
        let upper_bound = 10.0;
        let meter = Meter::new(upper_bound, "").unwrap();
        for _ in 0..100_000 {
            assert!(meter.sample() < upper_bound);
        }
    }

    #[test]
    #[serial]
    /// Tests if the functions `publish_sample` and `publish_simulation_end` of the `Meter` struct
    /// correctly send messages to the broker.
    fn test_meter_publish() {
        let upper_bound = 10.0;
        let url = "amqp://guest:guest@localhost:5672";
        let meter = Meter::new(upper_bound, url).unwrap();
        let mut samples = Vec::new();
        for _ in 0..50 {
            let time = Utc::now();
            let sample = meter.publish_sample(time).unwrap();
            samples.push((time, sample));
        }
        // End the simulation.
        meter.publish_simulation_end().unwrap();
        // Setup a consumer for the sent messages.
        let mut connection = Connection::insecure_open(url).unwrap();
        let channel = connection.open_channel(None).unwrap();
        let queue = channel.queue_declare(METER_ROUTING_KEY, QueueDeclareOptions::default()).unwrap();
        let consumer = queue.consume(ConsumerOptions::default()).unwrap();
        for (i, message) in consumer.receiver().iter().enumerate() {
            match message {
                ConsumerMessage::Delivery(delivery) => {
                    let message: BrokerMessage = serde_json::from_slice(&delivery.body).unwrap();
                    consumer.ack(delivery).unwrap();
                    if message.is_simulation_end() {
                        consumer.cancel().unwrap();
                    } else {
                        let message_values = (
                            message.time_stamp.unwrap(),
                            message.power_consumption.unwrap()
                        );
                        assert_eq!(samples[i].0, message_values.0);
                        // Do not assume bit exact serialisation.
                        assert!(float_compare_non_exact(samples[i].1, message_values.1));
                    }
                },
                ConsumerMessage::ClientCancelled => break,
                other => panic!("Consumer did not expect: {:?}", other),
            }
        }
        connection.close().unwrap();
    }

    #[test]
    /// Tests if the function `new` of the `BrokerMessage` struct only creates valid
    /// `BrokerMessage`s.
    fn test_broker_message_new() {
        // General testing.
        {
            let consumption = 1000.0;
            let time = Utc::now();
            let message = BrokerMessage::new(consumption, time);
            assert!(message.is_ok());
            let message = message.unwrap();
            assert_eq!(message.power_consumption, Some(consumption));
            assert_eq!(message.time_stamp, Some(time));
        }
        // Specific testing.
        assert!(BrokerMessage::new(0.0, Utc::now()).is_ok());
        assert!(BrokerMessage::new(-10.0, Utc::now()).is_err());
        assert!(BrokerMessage::new(f64::INFINITY, Utc::now()).is_err());
        assert!(BrokerMessage::new(f64::NEG_INFINITY, Utc::now()).is_err());
        assert!(BrokerMessage::new(f64::NAN, Utc::now()).is_err());
    }

    #[test]
    /// Tests if the function `simulation_end_message` of the `BrokerMessage` struct
    /// creates a message indiciating the end of the simulation.
    fn test_broker_message_simulation_end_message() {
        assert!(BrokerMessage::simulation_end_message().is_simulation_end());
    }

    #[test]
    /// Tests if the function `is_simulation_end` of the `BrokerMessage` struct
    /// correctly recognises messages indiciating the end of the simulation.
    fn test_broker_message_is_simulation_end() {
        let consumption = 1000.0;
        let time = Utc::now();
        // A conventional message.
        {
            let message = BrokerMessage{
                power_consumption: Some(consumption),
                time_stamp: Some(time),
            };
            assert!(!message.is_simulation_end());
        }
        // Time stamp is not strictly required.
        {
            let message = BrokerMessage{
                power_consumption: Some(consumption),
                time_stamp: None,
            };
            assert!(!message.is_simulation_end());
        }
        // Simulation end is indicated by absent power values.
        {
            let message = BrokerMessage{
                power_consumption: None,
                time_stamp: Some(time),
            };
            assert!(message.is_simulation_end());
        }
        {
            let message = BrokerMessage{
                power_consumption: None,
                time_stamp: None,
            };
            assert!(message.is_simulation_end());
        }
    }

    #[test]
    /// Tests if the getters of the `BrokerMessage` struct work as intended.
    fn test_broker_message_getters() {
        // Test message with information.
        {
            let consumption = 1000.0;
            let time = Utc::now();
            let message = BrokerMessage::new(consumption, time).unwrap();
            assert_eq!(Some(consumption), message.power_consumption());
            assert_eq!(Some(time), message.time_stamp());
        }
        // Test empty message.
        {
            let message = BrokerMessage::simulation_end_message();
            assert_eq!(None, message.power_consumption());
            assert_eq!(None, message.time_stamp());
        }
    }
}
