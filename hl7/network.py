import logging
import socket


logger = logging.getLogger(__name__)


def connect_and_send_message(data_to_send: bytes, port: int):
    """Connect to local server and send the message

    Parameters
    ----------
    data_to_send : bytes
        JSON data in bytes
    port : int
        Number port for local server
    """

    # handle connect and sending of JSON data to local server
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.connect(("127.0.0.1", port))
        except Exception as e:
            logger.exception(f"Failed connecting to 127.0.0.1:{port}")
            raise e
        else:
            # the newline marks the end of the size message so that the local
            # server can separate it from the data that follows
            s.sendall(f"Sending : {len(data_to_send)}\n".encode())
            s.sendall(data_to_send)

            received = s.recv(1024)
            received = received.decode("utf-8")
            logger.info(f"Received `{received}` from local server")
