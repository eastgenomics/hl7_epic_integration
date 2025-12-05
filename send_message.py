import argparse
import datetime
import json
import logging
from pathlib import PosixPath, Path
import socket
import time
from typing import Optional

import hl7apy
from hl7apy.parser import parse_message
import schedule


logger = logging.getLogger(__name__)

# MLLP framing characters
MLLP_START = "\x0b"
MLLP_END = "\x1c\r"


def schedule_job(paths: list, port: int, test: bool):
    """Schedule jobs for sending messages

    Parameters
    ----------
    paths : list
        List of paths in which messages need to be scheduled
    port : int
        Port number for local server
    """

    logger.info("Started job scheduling")

    for i in range(8, 18, 1):
        for day in ["monday", "tuesday", "wednesday", "thursday", "friday"]:
            getattr(schedule.every(), day).at(f"{i:02d}:00").do(
                main, paths, port, test
            )

    while True:
        schedule.run_pending()
        time.sleep(60)


def get_relevant_files(folder: PosixPath, test: bool) -> list:
    """Get the relevant files for the HL7 process i.e. files that are less than
    an hour old

    Parameters
    ----------
    folder : PosixPath
        Path containing the files to check or representing a file
    test : bool
        Bool to indicate the test mode

    Yields
    ------
    list
        List of files to be parsed and sent
    """

    TIME = datetime.datetime.now().timestamp()

    files = []

    if folder.is_file():
        return [folder]

    for file in folder.iterdir():
        if file.is_file():
            if not test:
                # get files that have been modified 1 hour ago at the
                # latest
                if TIME - int(file.stat().st_mtime) <= 3600:
                    files.append(file)
            else:
                files.append(file)

    return files


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


def wrap_with_mllp(message: str) -> str:
    """
    Wraps an HL7 message string with MLLP framing
    """
    return MLLP_START + message + MLLP_END


def connect_and_send_message(data_to_send: bytes, port: int):
    """Connect to local server and send the

    Parameters
    ----------
    data_to_send : bytes
        JSON data in bytes
    port : int
        Number port for local server
    """

    # handle connect and sending of JSON data to local server
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect(("127.0.0.1", port))

        s.sendall(f"Sending : {len(data_to_send)}".encode())
        s.sendall(data_to_send)

        received = s.recv(1024)
        received = received.decode("utf-8")
        logger.info(f"Received `{received}` from local server")


def main(paths: list, port: int, test: bool, scheduling: bool = False):
    """Gather, parse and send parsed content to local server

    Parameters
    ----------
    paths : list
        List of paths to files or folders in which message files are present
    port : int
        Port of the local server
    test : bool
        Boolean indicating whether to run the script in test mode i.e. does the
        script parse only files that have been here for the past hour
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

    logger.info(f"Arguments used: {paths} | {port} | {test} | {scheduling}")

    if scheduling:
        schedule_job(paths, port, test)

    list_paths = [p.name for p in paths]

    logger.info(f"Gathering files from '{", ".join(list_paths)}'")

    files = []

    for folder in paths:
        files += get_relevant_files(folder, test)

    if not files:
        logger.info(f"No files found in '{", ".join(list_paths)}'.")
        return

    messages = {}

    logger.info(f"Parsing '{", ".join([f.name for f in files])}'")

    for file in files:
        msg = parse_hl7_file(file)
        msg_er7 = str_to_er7_hl7_message(msg)

        if msg_er7 is not None:
            msg = wrap_with_mllp(msg_er7)
            messages[f"{file.resolve()}"] = msg

    data_to_send = json.dumps(messages).encode()

    connect_and_send_message(data_to_send, port)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("hl7_message_path", nargs="+", type=Path)
    parser.add_argument("local_port", type=int)
    parser.add_argument("-t", "--test", action="store_true", default=False)
    parser.add_argument("-s", "--schedule", action="store_true", default=False)
    args = parser.parse_args()
    main(args.hl7_message_path, args.local_port, args.test, args.schedule)
