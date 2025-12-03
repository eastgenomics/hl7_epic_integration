import argparse
import datetime
import json
import logging
from pathlib import PosixPath, Path
import socket
import sys
from typing import Optional

import hl7apy
from hl7apy.parser import parse_message


logger = logging.getLogger(__name__)


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


def main(paths: list, port: int, test: bool):
    logging.basicConfig(
        filename="hl7_sending_messages.log",
        level=logging.DEBUG,
        format="%(asctime)s - %(levelname)s - %(filename)s : %(funcName)20s() - %(message)s",
    )

    logger.info(f"Arguments used: {paths} | {port} | {test}")

    list_paths = [p.name for p in paths]

    logger.info(f"Gathering files from '{", ".join(list_paths)}'")

    files = []

    for folder in paths:
        files += get_relevant_files(folder, test)

    if not files:
        logger.info(f"No files found in '{", ".join(list_paths)}'.")
        sys.exit()

    messages = {}

    logger.info(f"Parsing '{", ".join([f.name for f in files])}'")

    for file in files:
        msg = parse_hl7_file(file)
        msg_er7 = str_to_er7_hl7_message(msg)

        if msg_er7 is not None:
            messages[f"{file.resolve()}"] = msg_er7

    data_to_send = json.dumps(messages).encode()

    # handle connect and sending of JSON data to local server
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect(("127.0.0.1", port))
        s.sendall(f"Sending : {len(data_to_send)}\n".encode())
        s.sendall(json.dumps(messages).encode())
        received = s.recv(1024)
        received = received.decode("utf-8")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("hl7_message_path", nargs="+", type=Path)
    parser.add_argument("local_port", type=int)
    parser.add_argument("-t", "--test", action="store_true", default=False)
    args = parser.parse_args()
    main(args.hl7_message_path, args.local_port, args.test)
