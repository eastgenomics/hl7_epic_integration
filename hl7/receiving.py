"""
Functions to parse, validate, save and acknowledge the HL7 messages
received from Epic
"""

from datetime import datetime
import logging
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
        return "NO_DATETIME", "NO_SPECIMEN", timestamp

    datetime_msg = message.msh.msh_7.value if message.msh.msh_7 else "NO_DATETIME"

    specimen = "NO_SPECIMEN"

    # the specimen ID field depends on whether the message is an order or
    # a result, so take the first ORC field that has a value
    if hasattr(message, "orc") and message.orc:
        for orc_field in [message.orc.orc_2, message.orc.orc_3, message.orc.orc_4]:
            # check value explicitly against None (handles value == 0)
            if orc_field is not None and orc_field.value is not None:
                # only keep the ID, not the other components of the field
                specimen = str(orc_field.value).split("^")[0]
                break

    return datetime_msg, specimen, timestamp


def save_message(
    message: str, output_dir: Path, datetime_msg: str, specimen: str, timestamp: str
) -> Path:
    """Save a HL7 message received into a txt file named
    "{datetime_msg}_{specimen}_{timestamp}.txt"

    Parameters
    ----------
    message : str
        HL7 message with segments separated by carriage returns
    output_dir : Path
        Directory to save the message in
    datetime_msg : str
        Datetime in the HL7 message
    specimen : str
        Specimen ID in the HL7 message
    timestamp : str
        Datetime at which the message was received

    Returns
    -------
    Path
        Path to the file the message was saved in
    """

    output_file = output_dir / f"{datetime_msg}_{specimen}_{timestamp}.txt"

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
        logger.warning(f"Message is missing segments: {sorted(missing_segments)}")
        return False

    return True


def create_ack(message: Message | None, valid: bool) -> str | None:
    """Create the ACK message to send back for a HL7 message received.
    A valid message gets an "AA" (accepted) ACK and an invalid message
    gets an "AE" (error) ACK.

    Parameters
    ----------
    message : Message | None
        Parsed HL7 message, or None if the message couldn't be parsed
    valid : bool
        Whether the message received is valid

    Returns
    -------
    str | None
        ACK message in ER7 format, or None if it couldn't be created
    """

    if message is None:
        logger.error("No ACK can be created for a message that couldn't be parsed")
        return None

    now = datetime.now().strftime("%Y%m%d%H%M%S")

    try:
        ack = Message("ACK", validation_level=VALIDATION_LEVEL.STRICT)

        # swap sender and receiver of the original message
        ack.msh.msh_3 = message.msh.msh_5.value
        ack.msh.msh_4 = message.msh.msh_6.value
        ack.msh.msh_5 = message.msh.msh_3.value
        ack.msh.msh_6 = message.msh.msh_4.value
        ack.msh.msh_7 = now
        ack.msh.msh_9 = "ACK"

        ack.add_segment("MSA")

        if valid:
            ack.msh.msh_10 = "ACK12345"
            ack.msa.msa_1 = "AA"
            # control ID of the message being acknowledged
            ack.msa.msa_2 = message.msh.msh_10.value
        else:
            ack.msh.msh_10 = f"ERR{now}"
            ack.msa.msa_1 = "AE"
            ack.msa.msa_3 = "Message validation failed: missing required segments"

        return ack.to_er7()

    except Exception:
        logger.exception("Couldn't create ACK message")
        return None
