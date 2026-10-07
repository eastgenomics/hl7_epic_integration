"""
Functions to parse, validate, save and acknowledge the HL7 messages
received from Epic
"""

import logging
import uuid
from datetime import datetime
from pathlib import Path

from hl7apy.consts import VALIDATION_LEVEL
from hl7apy.core import Message
from hl7apy.parser import parse_message

logger = logging.getLogger(__name__)

# segments a message needs to be considered valid
REQUIRED_SEGMENTS = {"MSH", "PID", "ORC"}


def parse_hl7_message(message: str) -> Message | None:
    """Parse a HL7 message

    Parameters
    ----------
    message : str
        HL7 message with segments separated by carriage returns

    Returns
    -------
    Message | None
        Parsed HL7 message, or None if the message couldn't be parsed
    """

    try:
        return parse_message(message, find_groups=False)
    except Exception:
        logger.exception("Couldn't parse HL7 message")
        return None


def get_message_details(message: Message | None) -> tuple:
    """Get the details used to name the file a message is saved in

    Parameters
    ----------
    message : Message | None
        Parsed HL7 message, or None if the message couldn't be parsed

    Returns
    -------
    tuple
        Datetime of the message (MSH-7), specimen ID (first ORC-2, ORC-3
        or ORC-4 field with a value) and the current timestamp.
        "NO_DATETIME" and "NO_SPECIMEN" are used for missing values.
    """

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    if message is None:
        return "NO_DATETIME", "NO_SAMPLE_ID", timestamp

    datetime_msg = (
        message.msh.msh_7.value if message.msh.msh_7 else "NO_DATETIME"
    )

    specimen = ""
    instrument_id = ""

    if hasattr(message, "orc") and message.orc:
        specimen = str(message.orc.orc_4.value).split("-")[-1]

    if hasattr(message, "zsp"):
        instrument_id = str(message.zsp.zsp_2.value)

    if all([specimen, instrument_id]):
        sample_id = f"{instrument_id}-{specimen}"
    else:
        sample_id = ""

    return datetime_msg, sample_id, timestamp


def save_message(
    message: str,
    output_dir: Path,
    datetime_msg: str,
    sample_id: str,
    timestamp: str,
) -> Path:
    """Save a HL7 message received into a txt file named
    "{datetime_msg}_{sample_id}_{timestamp}.txt". If that file already
    exists, a number is added to the name e.g.
    "{datetime_msg}_{sample_id}_{timestamp}_1.txt"

    Parameters
    ----------
    message : str
        HL7 message with segments separated by carriage returns
    output_dir : Path
        Directory to save the message in
    datetime_msg : str
        Datetime in the HL7 message
    sample_id : str
        Sample ID extracted from info in the HL7 message
    timestamp : str
        Datetime at which the message was received

    Returns
    -------
    Path
        Path to the file the message was saved in
    """

    file_name = f"{datetime_msg}_{sample_id}_{timestamp}"
    output_file = output_dir / f"{file_name}.txt"

    # don't overwrite a message with the same details received in the same
    # second (e.g. a message resent by Epic), add a number to the name instead
    counter = 1

    while output_file.exists():
        output_file = output_dir / f"{file_name}_{counter}.txt"
        counter += 1

    # save segments on separate lines to make the file readable
    output_file.write_text(message.replace("\r", "\n"))
    logger.info(f"Saved message in {output_file}")

    return output_file


def validate_message(message: Message | None) -> bool:
    """Check that a HL7 message has the required segments

    Parameters
    ----------
    message : Message | None
        Parsed HL7 message, or None if the message couldn't be parsed

    Returns
    -------
    bool
        True if the message contains all the required segments
    """

    if message is None:
        return False

    present_segments = {segment.name for segment in message.children}
    missing_segments = REQUIRED_SEGMENTS - present_segments

    if missing_segments:
        logger.warning(
            f"Message is missing segments: {sorted(missing_segments)}"
        )
        return False

    return True


def get_msh_fields(message: str) -> dict:
    """Get the MSH fields needed to create an ACK directly from the raw
    message, so that an ACK can be sent even if the message couldn't be
    parsed

    Parameters
    ----------
    message : str
        HL7 message with segments separated by carriage returns

    Returns
    -------
    dict
        MSH-3 to MSH-6 (sending/receiving application and facility) and
        MSH-10 (message control ID). Empty dict if the message has no MSH
        segment.
    """

    msh_segment = next(
        (
            segment
            for segment in message.split("\r")
            if segment.startswith("MSH")
        ),
        None,
    )

    if msh_segment is None:
        return {}

    # the field separator is the character right after "MSH". Once split,
    # MSH-1 is the separator itself so MSH-n is at index n - 1
    fields = msh_segment.split(msh_segment[3])

    return {
        f"msh_{n}": fields[n - 1]
        for n in (3, 4, 5, 6, 10)
        if len(fields) > n - 1 and fields[n - 1]
    }


def create_ack(
    message: str, ack_code: str, error_text: str = None
) -> str | None:
    """Create the ACK message to send back for a HL7 message received

    Parameters
    ----------
    message : str
        HL7 message received with segments separated by carriage returns
    ack_code : str
        "AA" (accepted), "AE" (error) or "AR" (rejected)
    error_text : str, optional
        Reason for the error, added to MSA-3 for "AE" and "AR" ACKs

    Returns
    -------
    str | None
        ACK message in ER7 format, or None if it couldn't be created
    """

    msh_fields = get_msh_fields(message)

    # swap sender and receiver of the original message
    swapped_fields = {
        "msh_3": "msh_5",
        "msh_4": "msh_6",
        "msh_5": "msh_3",
        "msh_6": "msh_4",
    }

    try:
        ack = Message("ACK", validation_level=VALIDATION_LEVEL.STRICT)

        for ack_field, original_field in swapped_fields.items():
            if original_field in msh_fields:
                setattr(ack.msh, ack_field, msh_fields[original_field])

        ack.msh.msh_7 = datetime.now().strftime("%Y%m%d%H%M%S")
        ack.msh.msh_9 = "ACK"
        # unique control ID for each ACK (max 20 characters)
        ack.msh.msh_10 = uuid.uuid4().hex[:20]

        ack.add_segment("MSA")
        ack.msa.msa_1 = ack_code

        # control ID of the message being acknowledged
        if "msh_10" in msh_fields:
            ack.msa.msa_2 = msh_fields["msh_10"]

        if error_text:
            ack.msa.msa_3 = error_text

        return ack.to_er7()

    except Exception:
        logger.exception("Couldn't create ACK message")
        return None
