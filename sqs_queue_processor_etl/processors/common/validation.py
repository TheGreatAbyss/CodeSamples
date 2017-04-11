from etl.services.logging import get_logger
from etl.lib.validation import MissingRequired, string

log = get_logger(__name__)


def sqs_string(value, length, column, nullable=False):

    # Raise the MissingRequired Error if value is Null when not supposed to be
    if value is None:
        if nullable:
            return None
        else:
            raise MissingRequired()

    v = str(value).strip()

    # If the length of the string is too long then log a warning and truncate
    if len(v) > length:
        log.warn("String for column {0}, has length of {1} which exceeds column length of {2}"
                 .format(column, len(v), length), extra={"string": v})

        return v[:length]

    else:
        return v
