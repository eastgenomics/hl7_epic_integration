"""
Helpers to add and remove the MLLP (Minimal Lower Layer Protocol) framing
used to send HL7 messages over TCP
"""

# MLLP framing characters
MLLP_START = b"\x0b"
MLLP_END = b"\x1c\r"


def remove_mllp_framing(data: bytes) -> str:
    """Remove the MLLP framing bytes from a HL7 message received over TCP
    and decode it

    Parameters
    ----------
    data : bytes
        HL7 message received, with or without MLLP framing bytes

    Returns
    -------
    str
        Decoded HL7 message with segments separated by carriage returns
    """

    # only strip the framing if the message is fully framed
    if data.startswith(MLLP_START) and data.endswith(MLLP_END):
        data = data[len(MLLP_START) : -len(MLLP_END)]

    # HL7 segments are separated by carriage returns
    return data.decode().replace("\n", "\r")


def wrap_with_mllp(message: str) -> bytes:
    """Wrap a HL7 message with the MLLP framing bytes to send it over TCP

    Parameters
    ----------
    message : str
        HL7 message

    Returns
    -------
    bytes
        Encoded HL7 message with MLLP framing bytes
    """

    return MLLP_START + message.encode("utf-8") + MLLP_END
