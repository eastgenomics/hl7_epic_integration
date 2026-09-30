"""
Receive HL7 messages from Epic over TCP (MLLP), save them to file and send
back an ACK message.

A FastAPI app runs alongside the TCP server so that the status of the
service can be checked over HTTP, e.g. `uvicorn hl7_receiving:app`
"""

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from logging.handlers import RotatingFileHandler
from pathlib import Path

from fastapi import FastAPI

from hl7 import mllp, receiving

logger = logging.getLogger(__name__)

# file to log to, can be set with the LOG_FILE environment variable
LOG_FILE = Path(os.environ.get("LOG_FILE", "hl7_receiving_messages.log"))
LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.DEBUG,
    format=(
        "%(asctime)s - %(levelname)7s - "
        "%(filename)18s : %(funcName)25s() - "
        "%(message)s"
    ),
    # log to file, starting a new file when it reaches 10 MB and keeping the
    # last 5 files
    handlers=[
        RotatingFileHandler(LOG_FILE, maxBytes=10 * 1024 * 1024, backupCount=5),
    ],
)

# TCP server configuration (host and port to listen to)
TCP_HOST = "0.0.0.0"
TCP_PORT = 20480

# maximum size of a single message, in bytes
MAX_MESSAGE_SIZE = 16 * 1024 * 1024

# directory to store the HL7 messages received, can be set with the
# RESPONSE_DIR environment variable
RESPONSE_DIR = Path(os.environ.get("RESPONSE_DIR", "./responses_dev"))


def process_message(data: bytes) -> str | None:
    """Save and validate a HL7 message received, and create the ACK to
    send back for it:
    - "AA" (accepted) if the message is valid
    - "AE" (error) if the message is missing required segments
    - "AR" (rejected) if the message couldn't be parsed

    Parameters
    ----------
    data : bytes
        HL7 message received, with or without MLLP framing bytes

    Returns
    -------
    str | None
        ACK message in ER7 format, or None if it couldn't be created
    """

    message_str = mllp.remove_mllp_framing(data)
    message = receiving.parse_hl7_message(message_str)

    # save every message received, even the invalid ones
    datetime_msg, specimen, timestamp = receiving.get_message_details(message)
    receiving.save_message(
        message_str, RESPONSE_DIR, datetime_msg, specimen, timestamp
    )

    if message is None:
        ack_code, error_text = "AR", "Message could not be parsed"
    elif not receiving.validate_message(message):
        ack_code = "AE"
        error_text = "Message validation failed: missing required segments"
    else:
        ack_code, error_text = "AA", None

    logger.info(f"Message for specimen {specimen} gets {ack_code} ACK")

    return receiving.create_ack(message_str, ack_code, error_text)


async def handle_tcp_connection(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter
):
    """Handle an incoming TCP connection and process the HL7 messages
    received through it until the client closes the connection.

    Messages are read one at a time up to the MLLP end bytes, so a message
    split over several TCP packets, or several messages sent at once, are
    handled correctly.

    Parameters
    ----------
    reader : asyncio.StreamReader
        Stream reader for the TCP connection
    writer : asyncio.StreamWriter
        Stream writer for the TCP connection
    """

    addr = writer.get_extra_info("peername")
    logger.info(f"Connection from {addr}")

    while True:
        # set when the client closes the connection
        connection_closed = False

        try:
            data = await reader.readuntil(mllp.MLLP_END)
        except asyncio.IncompleteReadError as e:
            # connection closed, process anything received without the
            # MLLP end bytes
            data = e.partial
            connection_closed = True
        except asyncio.LimitOverrunError:
            logger.error(
                f"Message from {addr} is bigger than {MAX_MESSAGE_SIZE} bytes, "
                "closing connection"
            )
            break

        if data.strip():
            logger.info(f"Received {len(data)} bytes from {addr}")
            ack = process_message(data)

            if ack:
                try:
                    writer.write(mllp.wrap_with_mllp(ack))
                    await writer.drain()
                    logger.info(f"Sent ACK to {addr}")
                except ConnectionError:
                    logger.warning(
                        f"Couldn't send ACK, {addr} closed the connection"
                    )
                    break

        if connection_closed:
            break

    logger.info(f"Connection closed from {addr}")
    writer.close()
    await writer.wait_closed()


async def start_tcp_server() -> asyncio.Server:
    """Start the TCP server listening for incoming HL7 messages

    Returns
    -------
    asyncio.Server
        TCP server listening on TCP_HOST:TCP_PORT

    Raises
    ------
    OSError
        Raised if the TCP server can't listen on the port, e.g. if the port
        is already in use
    """

    try:
        server = await asyncio.start_server(
            handle_tcp_connection,
            host=TCP_HOST,
            port=TCP_PORT,
            limit=MAX_MESSAGE_SIZE,
        )
    except OSError:
        logger.exception(f"TCP server couldn't listen on {TCP_HOST}:{TCP_PORT}")
        raise

    logger.info(f"TCP server listening on {TCP_HOST}:{TCP_PORT}")

    return server


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Run the TCP server in the background while the FastAPI app runs.
    If the TCP server can't start, the app doesn't start either.

    Parameters
    ----------
    app : FastAPI
        FastAPI app
    """

    RESPONSE_DIR.mkdir(parents=True, exist_ok=True)

    # start listening before the app starts so that a failure stops the app
    # instead of leaving the status check reporting that everything is fine
    server = await start_tcp_server()
    task = asyncio.create_task(server.serve_forever())

    # FastAPI app runs here
    yield

    # stop the TCP server when the FastAPI app shuts down
    task.cancel()

    try:
        await task
    except asyncio.CancelledError:
        pass

    server.close()
    await server.wait_closed()
    logger.info("TCP server stopped")


app = FastAPI(lifespan=lifespan)


@app.get("/")
async def read_root() -> dict:
    """HTTP endpoint to check the status of the service

    Returns
    -------
    dict
        Message with the server status
    """

    return {"message": "HTTP server is running alongside TCP listener"}
