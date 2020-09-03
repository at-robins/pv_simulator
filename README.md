# pv_simulator

Simulates a houshold power consumption meter, which sends messages to
a RabbitMQ broker, and a photovoltaic component, which obtains and processes
the messages form the broker. The results are written to a file in the JSON format.

## Requirements
Tested with:
- Python 3.8.2
- Rust 1.46.0
- RabbitMQ 3.8.2

## Build
After cloning the repository, the following commands must be
executed in the terminal:

```bash
cd path/to/pv_simulator
cargo build --release
cp target/release/libpv_simulator.so pv_simulator.so
```

## Usage
To display all command line parameters and their description:
```bash
cd path/to/pv_simulator
python3 pv_simulator_wrapper.py -h
```

## Example
To simulate 24 hours in intervalls of 5 seconds, using the message broker at URL "amqp://guest:guest@localhost:5672" and writing the simulated values to the file "pv_simulation_output.json":
```bash
cd path/to/pv_simulator
python3 pv_simulator_wrapper.py -s 5 -l 24 -b "amqp://guest:guest@localhost:5672" -o "./pv_simulation_output.json"
```
