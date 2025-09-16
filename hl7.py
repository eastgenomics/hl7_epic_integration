from datetime import datetime
from pathlib import Path

from fastapi import FastAPI
import asyncio

from hl7apy.exceptions import ValidationError

import hl7apy
from hl7apy.core import Message
from hl7apy.parser import parse_message
from hl7apy.core import Message
from hl7apy.consts import VALIDATION_LEVEL
from hl7apy.parser import parse_segment, parse_field, parse_component
from hl7apy.core import Message

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

#validate only 2 segments in an hl7 message received
def validate_message(data: str):
       
    m = parse_message(data, find_groups=False)
    required_segments = ['MSH', 'PID']
    segments=[segment.name for segment in m.children]
    for required_segment in required_segments:
        if required_segment not in segments:
            return False
        else:
            return True
            
#write an ack back 
def ack_message_back(original_message: str):
    try:
       
        msg = parse_message(original_message)

        
        ack = Message("ACK", validation_level=VALIDATION_LEVEL.STRICT)

        # Populate the MSH segment
        ack.msh.msh_3 = msg.msh.msh_5.value  # Swap sender/receiver
        ack.msh.msh_4 = msg.msh.msh_6.value
        ack.msh.msh_5 = msg.msh.msh_3.value
        ack.msh.msh_6 = msg.msh.msh_4.value
        ack.msh.msh_7 = datetime.now().strftime("%Y%m%d%H%M%S")
        ack.msh.msh_9 = 'ACK'
        ack.msh.msh_10 = 'ACK12345'

        
        ack.add_segment("MSA")
        ack.msa.msa_1 = "Reiceved"
        ack.msa.msa_2 = msg.msh.msh_10.value

        # Return the encoded ACK string
        return ack.to_er7()

    except Exception as e:
        print(f"Error generating ACK: {e}")
        return None
    

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

                
        if validate_message(message):
            print("HL7 message is valid")
            ack_hl7 = ack_message_back(message)

            if ack_hl7:
                writer.write(ack_hl7.encode())
                await writer.drain()
        else:
            print("Invalid HL7 message: missing required segments")

        write_to_file(data=message)

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