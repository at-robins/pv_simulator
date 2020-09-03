extern crate chrono;
extern crate serial_test;

pub use meter::{BrokerMessage, METER_ROUTING_KEY};
pub use pv_error::PvError;

use chrono::Duration;
use meter::Meter;
use photovoltaic_simulator::PvSimulator;
use simulated_time::SimulatedDateTime;
use std::path::{Path, PathBuf};
use std::thread;

pub fn simulate_pv_and_write_results_to_file<U: Into<String>, P: AsRef<Path>>(
    stride: Duration,
    simulation_length: Duration,
    broker_url: U,
    output_path: P) {

    // Use two different threads to simulate different, independent components of the system.
    // Variables for moving into the threads are created here.
    let broker_url_meter: String = broker_url.into();
    let broker_url_pv = broker_url_meter.clone();
    let output_pv: PathBuf = output_path.as_ref().into();

    // The first thread is the meter generating random values and passing them to the broker.
    let meter_sample_and_publish = thread::spawn(move || {
        // Create a meter with a range of 0-9000 W.
        // Unwrapping is not problematic as we know the upper bound
        // to be positive and finite.
        let meter = Meter::new(9000.0, broker_url_meter).unwrap();
        // Setup the time frame to be simulated.
        let simulation_time = SimulatedDateTime::new(stride, simulation_length);
        // Run the simulation.
        if let Err(err) = meter.publish_samples_to_broker_until(simulation_time) {
            // Use panic! to simplify function handling by the Python
            // wrapper. If this would be an actual library it should
            // return the result for proper error propagation.
            // This holds true for all subsequent code blocks.
            panic!("The meter simulation failed: {:?}", err);
        }
    });

    // The second thread is the pv simulator that gets the power consumption from
    // the broker auguments it and writes the results to a file.
    let pv_simulate_and_write = thread::spawn(move || {
        let mut simulator = PvSimulator::new(broker_url_pv);
        if let Err(err) = simulator.listen_to_broker() {
            panic!("Listening to the broker failed: {:?}", err);
        }
        if let Err(err) = simulator.write_records_to_file(output_pv) {
            panic!("Writing output to file failed: {:?}", err);
        }
    });

    // Wait for both of the threads to finish.
    if let Err(err) = meter_sample_and_publish.join() {
        panic!("The meter thread paniced: {:?}", err);
    }
    if let Err(err) = pv_simulate_and_write.join() {
        panic!("The pv simulator thread paniced: {:?}", err);
    }
    // Print a small notification that the simulation was finished.
    println!("    Simulation completed!");
}

/// Compares two floating point numbers for non-exact equality.
/// This method does not handle any corner cases.
///
/// # Parameters
///
/// * `first` - the first floating point number
/// * `second` - the second floating point number
fn float_compare_non_exact(first: f64, second: f64) -> bool {
    // Naive implementation for this specific simulation.
    // Precision for at least 6 decimal places is required.
    // Corner cases are irrelevant for this simulation as
    // they are check for during broker message creation.
    (first - second).abs() <= 0.000_000_1
}

#[cfg(test)]
mod tests {
    use serial_test::serial;
    use super::*;

    #[test]
    #[serial]
    /// Tests if the function `float_compare_non_exact` compares nearly equal floating point
    /// values correctly.
    fn test_simulate_pv_and_write_results_to_file() {
        let output = "./test_output.json";
        let url = "amqp://guest:guest@localhost:5672";
        let stride = Duration::seconds(5);
        let simulation_time = Duration::days(1);
        simulate_pv_and_write_results_to_file(
            stride,
            simulation_time,
            url,
            output)
    }

    #[test]
    /// Tests if the function `float_compare_non_exact` compares nearly equal floating point
    /// values correctly.
    fn test_float_compare_non_exact() {
        // Test equal equations.
        {
            let a = 0.15 + 0.15;
            let b = 0.1 + 0.2;
            assert!(a != b);
            assert!(float_compare_non_exact(a, b));
        }
        // Test inequal equations.
        {
            let a = 0.1 + 0.15;
            let b = 0.1 + 0.2;
            assert!(a != b);
            assert!(!float_compare_non_exact(a, b));
        }
    }
}

mod meter;
mod pv_error;
mod simulated_time;
mod photovoltaic_simulator;
