from datetime import datetime
from pathlib import Path

from fastapi import FastAPI
import asyncio

app = FastAPI()

TCP_HOST = "0.0.0.0"
TCP_PORT = 20480

# ensure output dir for responses exists
response_dir = Path('/appdata/epic_hl7_dev/responses/')
response_dir.mkdir(parents=True, exist_ok=True)


def write_to_file(data: str):
    filename = str(datetime.now().date()) + '_' + str(datetime.now().time()).replace(':', '-')
    
    with open(str(response_dir) + f'/{filename}.txt', "w+") as f:
        print(f'saving {filename} into directory {response_dir}')
        f.write(data)


async def handle_tcp_connection(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter
):
    addr = writer.get_extra_info("peername")
    print(f"Connection from {addr}")

    while True:
        data = await reader.read(1024)

        if not data:
            break

        message = data.decode()

        print(f"Received data: {message}")

        write_to_file(data=message)

        # Echo back (optional)
        writer.write(data)
        await writer.drain()

    print(f"Connection closed from {addr}")
    writer.close()
    await writer.wait_closed()


async def start_tcp_server():
    server = await asyncio.start_server(
        handle_tcp_connection,
        host=TCP_HOST,
        port=TCP_PORT,
    )
    print(f"TCP server listening on {TCP_HOST}:{TCP_PORT}")
    async with server:
        await server.serve_forever()


@app.on_event("startup")
async def startup_event():
    asyncio.create_task(start_tcp_server())


@app.get("/")
async def read_root():
    return {"message": "HTTP server is running alongside TCP listener"}