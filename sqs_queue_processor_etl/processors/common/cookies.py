import json
from etl.services.logging import get_logger
from urllib.parse import parse_qs

log = get_logger(__name__)


def extract_cookie_header(event):
    try:
        return event['request']['cookie']
    except KeyError as e:
        # Some events don't have cookie headers
        log.debug('No Cookie Key')
        return {}


def extract_cookie_dict(cookie_header):
    cookies = parse_qs(cookie_header, strict_parsing=False)
    return dict((k.strip(), v[0].strip()) for k, v in cookies.items())


def extract_cookies(event):
    cookie_header = extract_cookie_header(event)
    return extract_cookie_dict(cookie_header)


def extract_external_cookie_3_dict(json_message):
    external_cookie_3_str = extract_cookies(json_message).get('external_cookie_3', '{}')
    try:
        return json.loads(external_cookie_3_str)
    except json.JSONDecodeError:
        return {}