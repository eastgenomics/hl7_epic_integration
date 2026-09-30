"""
Receive HL7 messages from Epic over TCP (MLLP), save them to file and send
back an ACK message.

A FastAPI app runs alongside the TCP server so that the status of the
service can be checked over HTTP, e.g. `uvicorn hl7_receiving:app`
"""

import asyncio
from contextlib import asynccontextmanager
import logging
from pathlib import Path

from fastapi import FastAPI

from hl7 import mllp, receiving


logger = logging.getLogger(__name__)

logging.basicConfig(
    filename="hl7_receiving_messages.log",
    level=logging.DEBUG,
    format=(
        "%(asctime)s - %(levelname)7s - "
        "%(filename)18s : %(funcName)25s() - "
        "%(message)s"
    ),
)

# TCP server configuration (host and port to listen to)
TCP_HOST = "0.0.0.0"
TCP_PORT = 20480

# directory to store the HL7 messages received
RESPONSE_DIR = Path("./responses_dev")


async def handle_tcp_connection(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter
):
    """Handle an incoming TCP connection and process the HL7 messages
    received through it until the client closes the connection.

    Every message received is saved to file, then validated. An "AA" ACK
    is sent back for valid messages and an "AE" ACK for invalid ones.

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
        data = await reader.read(65536)

        if not data:
            # client closed the connection
            break

        logger.info(f"Received {len(data)} bytes from {addr}")

        message_str = mllp.remove_mllp_framing(data)
        message = receiving.parse_hl7_message(message_str)

        # save every message received, even the invalid ones
        datetime_msg, specimen, timestamp = receiving.get_message_details(message)
        receiving.save_message(
            message_str, RESPONSE_DIR, datetime_msg, specimen, timestamp
        )

        valid = receiving.validate_message(message)
        logger.info(f"Message for specimen {specimen} is {'valid' if valid else 'invalid'}")

        ack = receiving.create_ack(message, valid)

        if ack:
            writer.write(mllp.wrap_with_mllp(ack))
            await writer.drain()
            logger.info(f"Sent {'AA' if valid else 'AE'} ACK to {addr}")

    logger.info(f"Connection closed from {addr}")
    writer.close()
    await writer.wait_closed()


async def start_tcp_server():
    """Start the TCP server listening for incoming HL7 messages"""

    server = await asyncio.start_server(
        handle_tcp_connection, host=TCP_HOST, port=TCP_PORT
    )
    logger.info(f"TCP server listening on {TCP_HOST}:{TCP_PORT}")

    async with server:
        await server.serve_forever()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Run the TCP server in the background while the FastAPI app runs

    Parameters
    ----------
    app : FastAPI
        FastAPI app
    """

    RESPONSE_DIR.mkdir(parents=True, exist_ok=True)

    task = asyncio.create_task(start_tcp_server())

    # FastAPI app runs here
    yield

    # stop the TCP server when the FastAPI app shuts down
    task.cancel()

    try:
        await task
    except asyncio.CancelledError:
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
