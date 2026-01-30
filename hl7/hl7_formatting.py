import logging
import re

from hl7 import utils


# MLLP framing characters
MLLP_START = "\x0b"
MLLP_END = "\x1c\r"

logger = logging.getLogger(__name__)


def create_msh(timestamp: str) -> str:
    """Create the MSH segment for ORU^R01 messages

    Parameters
    ----------
    timestamp : str
        String representing the timestamp of the message

    Returns
    -------
    str
        MSH segment
    """

    return "|".join(
        [
            "MSH",
            "^~\\&",
            "Genomics",
            "218",
            "Epic",
            "Results",
            timestamp,
            "374",
            "ORU^R01",
            "45741",
            "T",
            "2.5.1",
            "",
            "",
            "",
            "AL",
            "AL",
        ]
    )


def wrap_with_mllp(message: str) -> str:
    """Wraps an HL7 message string with MLLP framing

    Parameters
    ----------
    message : str
        Message to wrap with MLLP characters

    Returns
    ------
    str
        Message wrapped with MLLP characters
    """

    return MLLP_START + message + MLLP_END


def format_segment(
    segments: list,
    segment_to_change: str,
    segment_count: int,
    change: str,
) -> list:
    """Make a change in a segment in the order message

    Parameters
    ----------
    segments : list
        List of segments from the order message
    segment_to_change : str
        Segment to change
    segment_count : int
        Position at which the change will take place
    change : str
        Value of the change

    Returns
    -------
    list
        _description_
    """
    segment_position = None
    new_segment = None

    for i, segment in enumerate(segments):
        if segment.startswith(segment_to_change):
            if segment_position is not None:
                logger.exception(
                    f"Multiple {segment_to_change} have been found"
                )

            segment_position = i
            new_segment = segment.split("|")
            new_segment[segment_count] = change

    if segment_position is None:
        logger.exception(
            f"No {segment_to_change} segment present in the order message"
        )
        raise Exception
    else:
        segments[segment_position] = "|".join(new_segment)

    return segments


def build_new_message(order_message: list, result_message: list) -> str:
    """Build new result message

    Parameters
    ----------
    order_message : list
        List containing all the segments of the order message
    result_message : list
        List containing all the segments of the result message

    Returns
    -------
    str
        Message ready to the be sent
    """

    new_message = []

    timestamp = utils.generate_timestamp()
    order_message = format_segment(order_message, "ORC", 1, "RE")
    order_message = format_segment(order_message, "OBR", 22, timestamp)
    new_message.append(create_msh(timestamp))
    new_message.extend(order_message)
    new_message.extend(result_message)
    new_message = "\r".join(new_message)
    new_message = wrap_with_mllp(new_message)

    return new_message
