from etl.services.logging import get_logger

log = get_logger(__name__)


def extract_headers(event):
    try:
        return event['eventData']['request']
    except KeyError:
        # log.warning("Event id %s missing eventData.request field")
        return {}
