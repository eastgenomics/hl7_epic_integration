import argparse
import datetime
import logging
from pathlib import PosixPath, Path
import socket
import time
from typing import Generator, Optional
import hl7apy
from hl7apy.parser import parse_message
import schedule


TIME = datetime.datetime.now().timestamp()

logger = logging.getLogger(__name__)


def get_relevant_files(
    folder: PosixPath, test: bool
) -> Generator[PosixPath, None, None]:
    """Get the relevant files for the HL7 process i.e. files that are less than
    an hour old

    Parameters
    ----------
    folder : PosixPath
        Path containing the files to check
    test : bool
        Bool to indicate the test mode

    Yields
    ------
    Generator[PosixPath, None, None]
        Generator for the files in the folder
        (in case there are a lot of files)
    """

    for file in folder.iterdir():
        if file.is_file():
            if not test:
                # get files that have been modified 1 hour ago at the
                # latest
                if TIME - int(file.stat().st_mtime) <= 3600:
                    yield file
            else:
                yield file


def parse_hl7_file(filepath: PosixPath) -> str:
    """Parse a file containing a HL7 message

    Parameters
    ----------
    filepath : PosixPath
        Path to the file to parse

    Returns
    -------
    str
        Content of the file concatenated
    """

    with open(filepath) as f:
        message = f.read()
        return message


def str_to_er7_hl7_message(msg: str) -> Optional[str]:
    """Parse a string message to a er7 formatted string. Skips files that fail
    parsing by the HL7apy package

    Parameters
    ----------
    msg : str
        Message extracted from the file

    Returns
    -------
    Optional[str]
        Either the mllp format message or None if the content of the file is
        not parsable
    """

    try:
        msg = parse_message(msg, find_groups=False)
        message = msg.to_er7()
        message = message.replace("\n", "\r").strip()
    except hl7apy.exceptions.ParserError:
        logger.error(f"Error while trying to parse message: {msg}")
        return
    else:
        return message


def wrap_with_mllp(message: str) -> bytes:
    """
    Wraps an HL7 message string with MLLP framing and returns as bytes
    """
    return MLLP_START + message.encode("utf-8") + MLLP_END


def schedule_job(
    epic_socket: socket.socket, messages: dict, host: str, port: int
):
    """Schedule jobs for sending messages

    Parameters
    ----------
    epic_socket : socket.socket
        Socket object
    messages : dict
        Dict of messages to send
    host : str
        String for the host to connect to
    port : int
        Port number
    """

    for i in range(8, 18, 1):
        for day in ["monday", "tuesday", "wednesday", "thursday", "friday"]:
            getattr(schedule.every(), day).at(f"{i:02d}:00").do(
                handle_connection, epic_socket, messages, host, port
            )

    while True:
        schedule.run_pending()
        time.sleep(60)


def connect_to_socket(
    socket: socket.socket, host: str, port: int
) -> socket.socket:
    """Connect to the specified host and port

    Parameters
    ----------
    socket : socket.socket
        Socket object
    host : str
        String for the host to connect to
    port : int
        Port number

    Returns
    -------
    socket.socket
        Socket object
    """

    socket.connect((host, port))
    return socket


def handle_connection(
    epic_socket: socket.socket, messages: dict, host: str, port: int
) -> Optional[None]:
    """Send messages and receive ACK message back

    Parameters
    ----------
    epic_socket : socket.socket
        Socket object connected to the Epic integration engine
    messages : dict
        Dict of messages to send
    host : str
        String for the host to connect to
    port : int
        Port number
    """

    logger.info("Trying to send messages")

    for source, msg in messages.items():
        attempt = 0
        success = False
        logger.info(f"Message from {source}")

        while attempt < 5:
            try:
                epic_socket.sendall(msg)
                data = epic_socket.recv(1024)

                if data:
                    logger.info(f"Received ack message back: {data}")
                else:
                    logger.info("No ACK message received from Epic")

                success = True
                break

            except BrokenPipeError:
                attempt += 1
                logger.error(
                    (
                        "Failed to send message (probably due to connection "
                        "reset on Epic side. Attempting to reconnect...)"
                    )
                )
                time.sleep(300)
                epic_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                epic_socket = connect_to_socket(epic_socket, host, port)

            except Exception as e:
                logger.exception(f"Error when trying to send the message: {e}")
                break

        if success is False:
            return


def main(paths: list, host: str, port: int, test: bool, start_schedule: bool):
    logging.basicConfig(
        filename="hl7_sending_messages.log",
        level=logging.DEBUG,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    )
    files = []

    for folder in paths:
        for file in get_relevant_files(folder, test):
            files.append(file)

    messages = {}

    for file in files:
        msg = parse_hl7_file(file)
        msg_er7 = str_to_er7_hl7_message(msg)

        if msg_er7 is None:
            continue

        hl7_msg = wrap_with_mllp(msg_er7)

        if hl7_msg:
            messages[file] = hl7_msg

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s = connect_to_socket(s, host, port)

        if start_schedule:
            schedule_job(s, messages, host, port)
        else:
            handle_connection(s, messages, host, port)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("hl7_message_path", nargs="+", type=Path)
    parser.add_argument("host")
    parser.add_argument("port", type=int)
    parser.add_argument("-t", "--test", action="store_true", default=False)
    parser.add_argument(
        "-s", "--start_schedule", action="store_true", default=False
    )
    args = parser.parse_args()
    main(
        args.hl7_message_path,
        args.host,
        args.port,
        args.test,
        args.start_schedule,
    )
