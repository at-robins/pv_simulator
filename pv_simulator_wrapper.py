"""A python wrapper for command line argument parsing and calling of
the rust library for simulation of a photovoltaic component."""
from argparse import ArgumentParser
import pv_simulator


def main():
    parser = ArgumentParser(description='Simulates a photovoltaic component.')
    parser.add_argument(
        "-s",
        "--stride",
        dest="stride",
        type=int,
        default=5,
        help="simulation intervall in sec",
        metavar="STRIDE"
    )
    parser.add_argument(
        "-l",
        "--length",
        dest="length",
        type=int,
        default=24,
        help="simulation length in h",
        metavar="LENGTH"
    )
    parser.add_argument(
        "-b",
        "--broker",
        dest="broker",
        default="amqp://guest:guest@localhost:5672",
        help="RabbitMQ message broker URL",
        metavar="BROKER"
    )
    parser.add_argument(
        "-o",
        "--output",
        dest="output",
        default="./pv_simulation_output.json",
        help="simulation output file in JSON format",
        metavar="OUT"
    )
    args = parser.parse_args()
    # No error handling required as the underlying rust library does this.
    pv_simulator.simulate_pv_and_write_results_to_file(
        args.stride,
        args.length,
        args.broker,
        args.output
    )


if __name__ == "__main__":
    main()
