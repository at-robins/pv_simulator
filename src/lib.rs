extern crate chrono;
extern crate cpython;
extern crate serial_test;

use chrono::Duration;
use cpython::{PyObject, PyResult, Python, py_module_initializer, py_fn};
use meter::Meter;
use photovoltaic_simulator::PvSimulator;
use simulated_time::SimulatedDateTime;
use std::path::{Path, PathBuf};
use std::thread;

// Add bindings for the Python wrapper.
py_module_initializer!(pv_simulator, |py, m| {
    m.add(py, "__doc__", "This module simulates a photovoltaic component in Rust.")?;
    m.add(py, "simulate_pv_and_write_results_to_file", py_fn!(
        py,
        simulate_pv_and_write_results_to_file_py(
            stride_in_sec: f64,
            simulation_length_in_h: f64,
            broker_url: String,
            output_path: String
        )
    ))?;
    Ok(())
});

/// The Python wrapper function corresponding to `simulate_pv_and_write_results_to_file`.
///
/// # Parameters
/// * `stride` - the simulated time steps in seconds
/// * `simulation_length` - the total simulation length in hours
/// * `broker_url` - the URL of the RabbitMQ message broker
/// * `output_path` - the path to the output file
///
/// # Panics
///
/// If any part of the simulation fails.
fn simulate_pv_and_write_results_to_file_py(
    py: Python,
    stride_in_sec: f64,
    simulation_length_in_h: f64,
    broker_url: String,
    output_path: String) -> PyResult<PyObject> {
        let stride = Duration::nanoseconds((stride_in_sec * 1_000_000_000.0) as i64);
        let simulation_length = Duration::nanoseconds((simulation_length_in_h * 3_600_000_000_000.0) as i64);
        simulate_pv_and_write_results_to_file(stride, simulation_length, broker_url, output_path);
        Ok(Python::None(py))
    }

/// Simulates the `Meter` and photovoltaic component as specified by the exercise's description.
/// The results are written to the specified file.
///
/// # Parameters
/// * `stride` - the simulated time steps
/// * `simulation_length` - the total simulation length
/// * `broker_url` - the URL of the RabbitMQ message broker
/// * `output_path` - the path to the output file
///
/// # Panics
///
/// If any part of the simulation fails.
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
pub fn float_compare_non_exact(first: f64, second: f64) -> bool {
    // Naive implementation for this specific simulation.
    // Precision for at least 6 decimal places is required.
    // Corner cases are irrelevant for this simulation as
    // they are check for during broker message creation.
    (first - second).abs() <= 0.000_000_1
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Utc};
    use serial_test::serial;
    use std::fs::File;
    use super::*;
    use super::photovoltaic_simulator::Record;

    #[test]
    #[serial]
    /// Tests if the function `simulate_pv_and_write_results_to_file` performes correctly.
    fn test_simulate_pv_and_write_results_to_file() {
        let output = "./test_output.json";
        let url = "amqp://guest:guest@localhost:5672";
        let stride = Duration::seconds(5);
        let simulation_time = Duration::days(1);
        let time_stamps: Vec<DateTime<Utc>> = SimulatedDateTime::new(stride, simulation_time)
            .collect();
        simulate_pv_and_write_results_to_file(
            stride,
            simulation_time,
            url,
            output
        );
        let records: Vec<Record> = serde_json::from_reader(File::open(output).unwrap()).unwrap();
        // Make sure the expected amount of records were outputted.
        assert_eq!(records.len(), time_stamps.len());
        // Test everything that was specified in the exercise's description.
        for (i, record) in records.iter().enumerate() {
            // The check is only performed at second precision since there is a small
            // delay between creation of reference and tested simulated time.
            assert_eq!(record._time_stamp().timestamp(), time_stamps[i].timestamp());
            // Power consumption must be between 0 and 9000 watt.
            assert!(record._power_consumption() <= 9000.0 && record._power_consumption() >= 0.0);
            // Th diagram showed a rough output range of 0 to 3500 watt.
            assert!(record._power_output() <= 3500.0 && record._power_output() >= 0.0);
            // Power consumption and ouput are sign-inverse and it was not specified if total
            // output or consumption is to be computed, so the absolute difference of both
            // values is compared with 6 decimal places of precision.
            assert!(float_compare_non_exact(
                record._total_power_output().abs(),
                (record._power_output() - record._power_consumption()).abs()
            ))
        }
        // Remove the test output file.
        std::fs::remove_file(output).expect("The test output file could not be removed.");
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
