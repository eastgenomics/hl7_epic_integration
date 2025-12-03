import argparse
import json
import logging
from pathlib import Path
import select
import socket
import sys
import re
import time

import schedule

import send_message

# port for local communication with the send_message.py script
LOCAL_PORT = 2000


logger = logging.getLogger(__name__)


def schedule_job(paths: list):
    """Schedule jobs for sending messages

    Parameters
    ----------
    paths : list
        List of paths in which messages need to be scheduled
    """

    for i in range(8, 18, 1):
        for day in ["monday", "tuesday", "wednesday", "thursday", "friday"]:
            getattr(schedule.every(), day).at(f"{i:02d}:00").do(
                send_message.main, paths, LOCAL_PORT, True
            )


def connect_to_socket(
    sock: socket.socket, host: str, port: int
) -> socket.socket:
    """Connect to the specified host and port

    Parameters
    ----------
    sock : socket.socket
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

    sock.connect((host, port))
    return sock


def send_message_to_epic(
    epic_socket: dict, messages: dict, host: str, port: int
):
    """Send message to epic

    Parameters
    ----------
    epic_socket : dict
        Dict containing the socket to use
    messages : dict
        Dict containing the message and its origin
    host : str
        Host IP for the Epic environment
    port : int
        Port for the Epic environment
    """

    logger.info("Trying to send messages")

    for source, msg in messages.items():
        attempt = 0
        success = False
        logger.info(f"Message from {source}")

        # attempt to reconnect 5 times to the host if the connection is reset
        # on their side
        while attempt < 5:
            try:
                epic_socket["socket"].sendall(msg.encode())
                data = epic_socket["socket"].recv(1024)

                if data:
                    logger.info(f"Received ack message back: {data}")
                else:
                    logger.info("No ACK message received from Epic")

                success = True
                break

            except BrokenPipeError:
                attempt += 1
                logger.exception(
                    (
                        "Failed to send message (probably due to connection "
                        "reset on Epic side. Attempting to reconnect in 5 "
                        f"minutes: {attempt}/5)"
                    )
                )
                time.sleep(300)
                new_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                epic_socket["socket"] = connect_to_socket(
                    new_socket, host, port
                )

            except Exception as e:
                logger.exception(f"Error when trying to send the message: {e}")
                break

        if success is False:
            return


def main(host: str, port: int, paths: list):
    """Main function to connect to Epic, start the local server and if
    necessary start the scheduling of jobs

    Parameters
    ----------
    host : str
        Host IP for Epic
    port : int
        Port for Epic
    paths : list
        Paths from which to start the scheduling from
    """

    logging.basicConfig(
        filename="hl7_sending_messages.log",
        level=logging.DEBUG,
        format=(
            "%(asctime)s - %(levelname)7s - "
            "%(filename)18s : %(funcName)20s() - "
            "%(message)s"
        ),
    )

    logger.info(f"Command line: `{' '.join(sys.argv)}`")

    epic_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    epic_socket_holder = {"socket": connect_to_socket(epic_socket, host, port)}
    logger.info(f"Initial connection to {host}:{port}")

    if paths:
        logger.info("Starting scheduled jobs")
        schedule_job(paths)

    # setup the local server
    local_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    local_server.bind(("127.0.0.1", LOCAL_PORT))
    local_server.listen(1)
    logger.info("Started local server")

    size_data = 0

    read_list = [local_server]

    while True:
        # allows for the while loop to not get stuck on the acceptance of
        # socket connection and allowing the scheduling to get triggered
        readable, writable, errored = select.select(read_list, [], [], 60)

        for s in readable:
            if s is local_server:
                conn, _ = local_server.accept()
                data = conn.recv(1024).decode().strip()
                data_msg = ""

                if data:
                    size_info = re.search(
                        r"(?P<sending>Sending : )(?P<size>[0-9]+)(?P<start_data>.*)",
                        data,
                    )

                    # Received message indicating the size of the next message
                    if size_info:
                        size_data = int(size_info.group("size"))
                        logger.debug(
                            f"Data will be {size_data} bytes of length"
                        )
                        conn.sendall("Received size data".encode())
                        # actual data is mixed in with the size of data
                        # message, so extract the bytes with the actual start
                        # of the message and add it to the final result
                        data_msg += size_info.group("start_data")

                    bytes_received = len(data_msg.encode())

                    while size_data > bytes_received:
                        # while there is data left in the next message,
                        # continue receiving data
                        data_chunk = conn.recv(1024)

                        if not data_chunk:
                            break

                        data_msg += data_chunk.decode()
                        bytes_received += len(data_chunk)

                    if len(data_msg.encode()) != size_data:
                        logger.error(
                            "Length of data received doesn't match size "
                            "information received ahead of time: "
                            f"{len(data_msg.encode())} != {size_data}"
                        )
                    else:
                        try:
                            json_data = json.loads(data_msg)
                        except json.JSONDecodeError:
                            logger.exception(
                                f"Received {data_msg} but not in JSON format"
                            )
                        else:
                            conn.sendall("Received data".encode())
                            send_message_to_epic(
                                epic_socket_holder, json_data, host, port
                            )

                    size_data = 0

                conn.close()

        if schedule.get_jobs():
            # run the scheduling
            schedule.run_pending()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("host", default="test")
    parser.add_argument("port", type=int)
    parser.add_argument("-p", "--paths", nargs="+", type=Path)
    args = parser.parse_args()
    main(args.host, args.port, args.paths)
