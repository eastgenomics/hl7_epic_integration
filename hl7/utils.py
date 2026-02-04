import datetime
import logging
from pathlib import PosixPath
import time

import schedule


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

    Returns
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


def parse_hl7_file(filepath: PosixPath) -> list:
    """Parse a file containing a HL7 message

    Parameters
    ----------
    filepath : PosixPath
        Path to the file to parse

    Returns
    -------
    str
        Content of the file concatenated using carriage returns instead of
        newlines
    """

    with open(filepath) as f:
        message = f.read()
        return message.split("\n")


def grab_relevant_segments(message: list) -> list:
    """From the parsed hl7 message, get the relevant segments for a result
    message

    Parameters
    ----------
    message : list
        Parsed HL7 message

    Returns
    -------
    list
        List of all the relevant segments to be used for the result message
    """

    new_message = []
    relevant_segments = ("PID", "ORC", "OBR")

    for segment in message:
        if segment.startswith(relevant_segments):
            new_message.append(segment)

    return new_message


def generate_timestamp() -> str:
    """Generate timestamp in YYYYMMDDHHmmSS format

    Returns
    -------
    str
        String of the timestamp
    """

    return datetime.datetime.today().strftime("%Y%m%d%H%M%S")


def schedule_job(
    order_message_path: PosixPath,
    result_message_path: PosixPath,
    port: int,
    test: bool,
):
    """Schedule jobs for sending messages

    Parameters
    ----------
    order_message_path : PosixPath
        Path to the order message
    result_message_path : PosixPath
        Path to the result message
    port : int
        Port number for local server
    test : bool
        Boolean to indicate whether to use test mode for gathering files
    """

    from send_message import main

    logger.info("Started job scheduling")

    for i in range(8, 18, 1):
        for day in ["monday", "tuesday", "wednesday", "thursday", "friday"]:
            getattr(schedule.every(), day).at(f"{i:02d}:00").do(
                main, order_message_path, result_message_path, port, test
            )

    while True:
        schedule.run_pending()
        time.sleep(60)
