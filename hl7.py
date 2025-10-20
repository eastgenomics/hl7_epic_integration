from datetime import datetime
from pathlib import Path
from fastapi import FastAPI
import os
import asyncio
from hl7apy.core import Message
from hl7apy.parser import parse_message
from hl7apy.core import Message
from hl7apy.consts import VALIDATION_LEVEL
from hl7apy.core import Message
from contextlib import asynccontextmanager


# TCP server configuration (port to listen to)
TCP_HOST = "0.0.0.0"
TCP_PORT = 20480

# MLLP framing characters 
MLLP_START = b'\x0b'
MLLP_END = b'\x1c\r'

# Directory to store HL7 messages
response_dir = "./responses_dev"

def remove_mllp_framing_bytes(data: bytes) -> str:
    """
    Adds of removes MLLP protocol framing start and end bytes from
    an HL7 message and decodes it

    Parameters
    ----------
    data : bytes
        hl7 message received with framing bytes

    Returns
    ---------
    string:
          stripped hl7 message and decoded
    """
    if data.startswith(MLLP_START) and data.endswith(MLLP_END):
        data = data[1:-2]
    message = data.decode('utf-8')
    return  message.replace('\n','\r')

def wrap_with_mllp(message: str) -> bytes:
    """
    Wraps an HL7 message string with MLLP framing and returns as bytes
    """
    return MLLP_START + message.encode("utf-8") + MLLP_END


def write_to_file(data: str):
    """
    Save a hl7 message received into a txt file

    Parameters
    ----------
    data : string
        hl7 message received
    """
    os.makedirs(response_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    with open(str(response_dir) + f'/{timestamp}.txt', "w+") as f:
        print(f'saving {timestamp} into directory {response_dir}')
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
    except Exception as e:
        print(f"Validation error: {e}")
        return False
            

def ack_message_back(original_message: str):
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
    
def create_error_ack(original_message: str):
    """
    Create an HL7 error ACK message for invalid messages
    
    Parameters
    ----------
    original_message : str
        Original HL7 message that failed validation
        
    Returns
    -------
    str
        HL7 error ACK message in ER7 format
    """

    try:

        msg = parse_message(original_message)
        
        ack = Message("ACK", validation_level=VALIDATION_LEVEL.STRICT)
        
        # Populate MSH segment
        ack.msh.msh_3 = msg.msh.msh_5.value  # Swap sender/receiver
        ack.msh.msh_4 = msg.msh.msh_6.value
        ack.msh.msh_5 = msg.msh.msh_3.value
        ack.msh.msh_6 = msg.msh.msh_4.value
        ack.msh.msh_7 = datetime.now().strftime("%Y%m%d%H%M%S")
        ack.msh.msh_9 = 'ACK'
        ack.msh.msh_10 = 'ERR' + datetime.now().strftime("%Y%m%d%H%M%S")
        
        # Create error acknowledgment
        ack.add_segment("MSA")
        ack.msa.msa_1 = "AE"
        ack.msa.msa_3 = "Message validation failed: missing required segments"
        
        return ack.to_er7()
        
    except Exception as e:
        print(f"Error generating error ACK: {e}")
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
                writer.write(wrap_with_mllp(ack_hl7))
                await writer.drain()
        else:

            print("Invalid HL7 message: missing required segments")
            error_ack = create_error_ack(message)
            if error_ack:
                writer.write(wrap_with_mllp(error_ack))
                await writer.drain()

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

# FastAPI app and TCP server running together
@asynccontextmanager
async def lifespan():

    task = asyncio.create_task(start_tcp_server())
    
    yield # fastapi will run it

    try:
        await task
    except asyncio.CancelledError:
        print("TCP server not running")

app = FastAPI(lifespan=lifespan)

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