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
    10 minutes old

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
            # if not test:
            #     # get files that have been modified 10 minutes ago at the
            #     # latest
            #     if TIME - int(file.stat().st_mtime) <= 600:
            #         yield file
            # else:
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
        return "\r".join([line.strip() for line in f.readlines()])


def str_to_mllp_hl7_message(msg: str) -> Optional[str]:
    """Parse a string message to a mllp formated string. Skips files that fail
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
        msg = parse_message(msg)
    except hl7apy.exceptions.ParserError:
        logger.error(f"Error while trying to parse message: {msg}")
        return
    else:
        return msg.to_mllp()


def schedule_job(epic_socket: socket.socket, messages: list):
    """Schedule jobs for sending messages

    Parameters
    ----------
    epic_socket : socket.socket
        Socket object
    messages : list
        List of messages to send
    """

    for i in range(8, 18, 1):
        schedule.every().monday.at(f"{i:02d}:00").do(
            handle_connection, epic_socket, messages
        )
        schedule.every().tuesday.at(f"{i:02d}:00").do(
            handle_connection, epic_socket, messages
        )
        schedule.every().wednesday.at(f"{i:02d}:00").do(
            handle_connection, epic_socket, messages
        )
        schedule.every().thursday.at(f"{i:02d}:00").do(
            handle_connection, epic_socket, messages
        )
        schedule.every().friday.at(f"{i:02d}:00").do(
            handle_connection, epic_socket, messages
        )

    while True:
        schedule.run_pending()
        time.sleep(60)


def handle_connection(epic_socket: socket.socket, messages: list):
    """Send messages and receive ACK message back

    Parameters
    ----------
    epic_socket : socket.socket
        Socket object connected to the Epic integration engine
    messages : list
        List of messages to send
    """

    for msg in messages:
        logger.info("Trying to send messages")

        try:
            epic_socket.sendall(msg.encode("utf-8"))
            data = epic_socket.recv(1024)

            if data:
                logger.info(f"Received ack message back: {data}")
        except Exception as e:
            logger.exception(f"Error when trying to send the message: {e}")
        else:
            logger.info("Successfully sent message")


def test_function(messages: list):
    """Test function to test the scheduling package

    Parameters
    ----------
    messages : list
        List of messages to send out
    """

    schedule.every().minute.do(print, "test")

    while True:
        schedule.run_pending()
        time.sleep(10)


def main(paths: list, host: str, port: int, test: bool, schedule: bool):
    logging.basicConfig(
        filename="hl7_sending_messages.log",
        level=logging.DEBUG,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    )
    files = []

    for folder in paths:
        for file in get_relevant_files(folder, test):
            files.append(file)

    messages = []

    for file in files:
        msg = parse_hl7_file(file)
        hl7_msg = str_to_mllp_hl7_message(msg)

        if hl7_msg:
            messages.append(hl7_msg)

    if test:
        test_function(messages)
    else:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((host, port))

            if schedule:
                schedule_job(s, messages)
            else:
                handle_connection(s, messages)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("hl7_message_path", nargs="+", type=Path)
    parser.add_argument("host")
    parser.add_argument("port", type=int)
    parser.add_argument("-t", "--test", action="store_true", default=False)
    parser.add_argument("-s", "--schedule", action="store_true", default=False)
    args = parser.parse_args()
    main(args.hl7_message_path, args.host, args.port, args.test, args.schedule)
