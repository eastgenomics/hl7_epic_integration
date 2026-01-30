import argparse
import json
import logging
from pathlib import PosixPath

from hl7 import hl7_formatting, network, utils


logger = logging.getLogger(__name__)


def main(
    order_message_path: PosixPath,
    result_message_path: PosixPath,
    port: int,
    test: bool,
    scheduling: bool = False,
):
    """Gather, parse and send parsed content to local server

    Parameters
    ----------
    order_message_path : PosixPath
        Path to the order message
    result_message_path : PosixPath
        Path to the result message
    port : int
        Port of the local server
    test : bool
        Boolean indicating whether to run the script in test mode i.e. does the
        script parse only files that have been here for the past hour
    scheduling : bool
        Boolean to indicate whether to start scheduling of messages.
        Defaults to False
    """

    logging.basicConfig(
        filename="hl7_sending_messages.log",
        level=logging.DEBUG,
        format=(
            "%(asctime)s - %(levelname)7s - "
            "%(filename)18s : %(funcName)25s() - "
            "%(message)s"
        ),
    )

    logger.info(
        (
            "Arguments used:"
            f"{order_message_path} | "
            f"{result_message_path} | "
            f"{port} |"
            f"{test} | "
            f"{scheduling}"
        )
    )

    if scheduling:
        utils.schedule_job(order_message_path, result_message_path, port, test)

    logger.info(f"Parsing {order_message_path} and {result_message_path}")

    message_origin = {}

    order_message = utils.parse_hl7_file(order_message_path)
    relevant_hl7_segments = utils.grab_relevant_segments(order_message)
    result_message = utils.parse_hl7_file(result_message_path)
    msg_to_send = hl7_formatting.build_new_message(
        relevant_hl7_segments, result_message, test
    )

    message_origin[
        (f"{order_message_path.resolve()} + {result_message_path.resolve()}")
    ] = msg_to_send

    data_to_send = json.dumps(msg_to_send).encode()

    if not test:
        network.connect_and_send_message(data_to_send, port)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "This script sends messages to a local server using the given "
            "port. It can accomplish this using a schedule or sending one-off "
            "messages."
        )
    )
    parser.add_argument(
        "order_message_path", type=PosixPath, help="Path to the order message"
    )
    parser.add_argument(
        "result_message_path",
        type=PosixPath,
        help="Path to the result message",
    )
    parser.add_argument(
        "local_port",
        type=int,
        help=(
            "Local port to the local server as indicated in the "
            "epic_connection.py script"
        ),
    )
    parser.add_argument(
        "-t",
        "--test",
        action="store_true",
        default=False,
        help=(
            "Boolean to indicate to gather files from the last hour in the "
            "given paths"
        ),
    )
    parser.add_argument(
        "-s",
        "--schedule",
        action="store_true",
        default=False,
        help=(
            "Boolean to indicate whether to start sending messages on a "
            "schedule"
        ),
    )
    args = parser.parse_args()
    main(
        args.order_message_path,
        args.result_message_path,
        args.local_port,
        args.test,
        args.schedule,
    )
