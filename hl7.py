from datetime import datetime
from pathlib import Path

import asyncio

from hl7apy.core import Message
from hl7apy.parser import parse_message
from hl7apy.core import Message
from hl7apy.consts import VALIDATION_LEVEL
from hl7apy.core import Message

# define host and port to listen to
TCP_HOST = "0.0.0.0"
TCP_PORT = 20480

# ensure output dir for responses exists
response_dir = Path('/appdata/epic_hl7_dev/responses/')
response_dir.mkdir(parents=True, exist_ok=True)


def write_to_file(data: str):
    """
    Save a hl7 message received into a txt file

    Parameters
    ----------
    data : string
        hl7 message received
    """
    
    filename = str(datetime.now().date()) + '_' + str(datetime.now().time()).replace(':', '-')
    
    with open(str(response_dir) + f'/{filename}.txt', "w+") as f:
        print(f'saving {filename} into directory {response_dir}')
        f.write(data)


def validate_message(data: str) -> bool:
    """
    Validate a hl7 message received by checking it has the required segments

    Parameters
    ----------
    data : string
        hl7 message received

    Returns
    -------
    bool: 
        merged dataframe
    """
       
    try:
        m = parse_message(data, find_groups=False)
        required_segments = {'MSH', 'PID'}
        present_segments = {segment.name for segment in m.children}
        return required_segments.issubset(present_segments)
    except Exception:
        return False
            

def ack_message_back(original_message: str) -> str:
    """
    Create an hl7 message as an ACK from the original hl7 message received 

    Parameters
    ----------
    original_message : string
        hl7 message received

    Returns
    -------
    str:
        HL7 ACK message in ER7 format
    """
       
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

        # Create acknowledgment 
        ack.add_segment("MSA")
        ack.msa.msa_1 = "AA"
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
 
    """
    Handles an incoming TCP connection and processes HL7 messages.

    Reads data from the client over TCP.
    Validates the HL7 message.
    Sends back an HL7 ACK message if the input is valid.
    Logs invalid messages and saves all incoming messages to file.

    Parameters
    ----------
    reader : asyncio.StreamReader
        Stream reader for the TCP connection.

    writer : asyncio.StreamWriter
        Stream writer for the TCP connection.
    """

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

# Start the TCP server to listen for incoming HL7 messages.
async def start_tcp_server():

    server = await asyncio.start_server(
        handle_tcp_connection,
        host=TCP_HOST,
        port=TCP_PORT,
    )
    print(f"TCP server listening on {TCP_HOST}:{TCP_PORT}")
    async with server:
        await server.serve_forever()

# Set the function run automatically when the application starts
@app.on_event("startup")
async def startup_event():
    """
    Schedule a routine task (start the server)
    """
    asyncio.create_task(start_tcp_server())


# Endpoint for HTTP connection
@app.get("/")
async def read_root() -> dict:
    """
    Returns
    -------
    dict:
        message with the server status
    """

    return {"message": "HTTP server is running alongside TCP listener"}