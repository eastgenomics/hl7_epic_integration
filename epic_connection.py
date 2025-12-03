import argparse
import json
import logging
import socket
import sys
import time

import schedule

import send_message

LOCAL_PORT = 2000
# MLLP framing characters
MLLP_START = b"\x0b"
MLLP_END = b"\x1c\r"


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


def wrap_with_mllp(message: str) -> bytes:
    """
    Wraps an HL7 message string with MLLP framing and returns as bytes
    """
    return MLLP_START + message.encode("utf-8") + MLLP_END


def send_message_to_epic(epic_socket, messages, host, port):
    logger.info("Trying to send messages")

    for source, msg in messages.items():
        attempt = 0
        success = False
        logger.info(f"Message from {source}")

        msg = wrap_with_mllp(msg)

        # attempt to reconnect 5 times to the host if the connection is reset
        # on their side
        while attempt < 5:
            try:
                epic_socket["socket"].sendall(msg)
                data = epic_socket["socket"].recv(1024)

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
                new_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                epic_socket["socket"] = connect_to_socket(
                    new_socket, host, port
                )

            except Exception as e:
                logger.exception(f"Error when trying to send the message: {e}")
                break

        if success is False:
            return


def main(host: str, port: int, paths):
    logging.basicConfig(
        filename="hl7_sending_messages.log",
        level=logging.DEBUG,
        format="%(asctime)s - %(levelname)s - %(filename)s : %(funcName)20s() - %(message)s",
    )
    logger.info(f"Command line: `{' '.join(sys.argv)}`")

    # epic_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # epic_socket_holder = {"socket": connect_to_socket(epic_socket, host, port)}
    # logger.info(f"Initial connection to {host}:{port}")

    if paths:
        logger.info("Starting scheduled jobs")
        schedule_job(paths)

    local_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    local_socket.bind(("127.0.0.1", LOCAL_PORT))
    local_socket.listen(1)
    logger.info("Started local server")

    size_data = 0

    while True:
        conn, _ = local_socket.accept()
        data = conn.recv(1024).decode().strip()

        if data:
            if data.startswith("Sending"):
                logger.debug(data)
                size_data = int(data.split(" : ")[1])
                data = ""

            while size_data > 0:
                size_data -= 1024
                data += conn.recv(1024).decode()
                # logger.debug(data)

            try:
                json_data = json.loads(data)
            except TypeError:
                logger.error(f"Received {data} but not in JSON format")
            else:
                logger.debug(f"Received {json_data}")
                conn.sendall("Message received".encode())
                # send_message_to_epic(epic_socket_holder, json.loads(data), host, port)

        size_data = 0

        conn.close()

        if schedule.get_jobs():
            # run the scheduling
            schedule.run_pending()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("host", default="test")
    parser.add_argument("port", type=int)
    parser.add_argument("-p", "--paths", nargs="+")
    args = parser.parse_args()
    main(args.host, args.port, args.paths)
