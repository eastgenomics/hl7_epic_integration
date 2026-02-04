from datetime import datetime
from fastapi import FastAPI
import asyncio
from hl7apy.core import Message
from hl7apy.parser import parse_message
from hl7apy.consts import VALIDATION_LEVEL
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
    Adds or removes MLLP protocol framing start and end bytes from
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
    message = None
    if data.startswith(MLLP_START) and data.endswith(MLLP_END):
        data = data[1:-2]
        message = data.decode("latin-1")
    else:
        message = data
    return  message.replace('\n','\r')

def wrap_with_mllp(message: str) -> bytes:
    """
    Wraps an HL7 message string with MLLP framing and returns as bytes
    """
    return MLLP_START + message.encode("utf-8") + MLLP_END


def get_file_name(data: str):   
    """
    Parses a raw hl7 message and decodes it to get attributes (datetime of message and specimen ID)

    Parameters
    ----------
    data : bytes
        hl7 message received with framing bytes

    Returns
    ---------
    strings:
           datetime, specimen id and timestamp
    """

    if data.startswith(MLLP_START) and data.endswith(MLLP_END):
        data = data[1:-2]
    message = data.decode("latin-1").replace('\n', '\r')

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S%f")

    try:
        m = parse_message(message, find_groups=False)
    except Exception as e:
        print(f"HL7 parse error in get_file_name(): {e}")
        return "NO_MESSAGE_TYPE", "NO_SPECIMEN", timestamp
    
    # Get order number (without container if possible)
    order=m.msh.msh_10.value
    order_number=order.split(".")[1]

    # Define if order or results messade
    message_type="NO_MESSAGE_TYPE"

    if "ORM" in m.msh.msh_9.value:
        message_type = "OR"
    elif "ORU" in m.msh.msh_9.value:
        message_type = "RE"

    # Get specimen ID
    specimen = "NO_SPECIMEN"

    if hasattr(m, "orc") and m.orc:
        if m.orc.orc_4 and m.orc.orc_4.valiue:
            specimen_id = m.orc.orc_4.value
        else:
            specimen_id = None
        
        if specimen_id:
            specimen = specimen_id.split("^")[0]

    # Get test type
    test_type = "NO_TEST_TYPE"

    if hasattr(m, "obr") and m.obr:
        if m.obr.obr_4 and m.obr.obr_4.value:
            test = m.obr.obr_4.value
        else:
            test = None
        
        if "RARE" in test:
            test_type = "RDA"
        elif "CEN" in test:
            test_type = "CENNGS"

    # Get test type
    message_id = "NO_ID"

    if hasattr(m, "obr") and m.obr:
        if m.obr.obr_2 and m.obr.obr_2.value:
            message = m.obr.obr_2.value
        else:
            message = None
        
        if message:
            message_id = message.split("^")[0]
    
    return message_id, specimen, message_type, test_type, timestamp, order_number


def write_to_file(data: str, message_id, specimen, message_type, test_type, timestamp, order_number):
    """
    Save a hl7 message received into a txt file

    Parameters
    ----------
    data : string
        hl7 message received with framing bytes
    datetime_msg: string
        date and time in the hl7 message
    specimen_id: string
        specimen ID in the hl7 message
    time_stamp: string
        date time at the moment the message is saved as a txt file
    
    """

    with open(f"{response_dir}/{message_id}_{specimen}_{message_type}_{test_type}_{timestamp}_{order_number}.txt", "w+") as f:
        print(f"Saving{message_id}_{specimen}_{message_type}_{test_type}_{timestamp}_{order_number} into directory {response_dir}")
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
        required_segments = {'MSH', 'PID', 'ORC'}
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
        ack.msh.msh_10 = msg.msh.msh_10.value

        # Create acknowledgment 
        ack.add_segment("MSA")
        ack.msa.msa_1 = "AA"
        ack.msa.msa_2 = "Message Validation Passed"

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
        ack.msh.msh_10 = msg.msh.msh_10.value
        
        # Create error acknowledgment
        ack.add_segment("MSA")
        ack.msa.msa_1 = "AE"
        ack.msa.msa_2 = "Message validation failed: missing required segments"
        
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
        data = await reader.read(65536)
        if not data:
            break

        message_id, specimen, message_type, test_type, timestamp, order_number = get_file_name(data)

        hl7_msg_str = remove_mllp_framing_bytes(data)
        hl7_msg = hl7_msg_str.replace('\r', '\n')
        write_to_file(hl7_msg, message_id, specimen, message_type, test_type, timestamp, order_number)

        if validate_message(hl7_msg_str):
            print("HL7 message is valid")
            ack_hl7 = ack_message_back(hl7_msg_str)
            print(f'Message valid {ack_hl7}')
            if ack_hl7:
                writer.write(wrap_with_mllp(ack_hl7))
                await writer.drain()
        else:
            print("Invalid HL7 message: missing required segments")
            error_ack = create_error_ack(hl7_msg_str)
            if error_ack:
                writer.write(wrap_with_mllp(error_ack))
                await writer.drain()


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
async def lifespan(app: FastAPI):

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