import os
import re
import pytz
from calendar import timegm
from datetime import datetime
from urllib.parse import unquote

import etl.services.redshift
from etl.lib.validation import clean_fields, boolean, date, integer
from etl.services.dynamodb import Pageviews
from etl.services.logging import get_logger
from etl.sqs_queue_processor.lib.rop import SQSFailure, SQSSuccess, SQSUntracked
from etl.sqs_queue_processor.processors.common.cookies import extract_cookies, extract_external_cookie_3_dict
from etl.sqs_queue_processor.processors.common.headers import extract_headers
from etl.sqs_queue_processor.processors.common.user_agent import extract_user_agent
from etl.sqs_queue_processor.processors.common.validation import sqs_string

log = get_logger(__name__)

TZ = pytz.timezone('US/Eastern')
UTCTZ = pytz.timezone('UTC')


def handle_page_view(rop):
    event = rop.record
    cookies = extract_cookies(event)
    headers = extract_headers(event)
    user_agent = extract_user_agent(event)

    to_datetime = lambda v: ts_to_dt(v) if v else None
    to_unquoted = lambda v: unquote(v) if v else None
    unlist = lambda v: v[0] if type(v) is list else v

    record = {
        'event_id': str(event.get('eventId')),
        'application_id': 'app-34k430rgkj3409349jf30f',
        'user_hashed_id': find_user_hash(event, headers),
        'user_id': event.get('userId'),
        'page': event.get('eventData', {}).get('page'),
        'internal_cookie_1': cookies.get('internal_cookie_1'),
        'internal_cookie_2': cookies.get('internal_cookie_2'),
        'event_date': to_datetime(event.get('eventData', {}).get('eventTime')).astimezone(UTCTZ).date(),
        'event_time': to_datetime(event.get('eventData', {}).get('eventTime')).astimezone(UTCTZ),
        # The testing_system_1 field, when present, is a list of dicts. The Mango code only saved the data from the first element in that list.
        'testing_system_1_experiment_id': to_unquoted(event.get('eventData', {}).get('testing_system_1', [{}])[0].get('experimentId')),
        'testing_system_1_experiment_name': to_unquoted(event.get('eventData', {}).get('testing_system_1', [{}])[0].get('experimentName')),
        'testing_system_1_variation_id': to_unquoted(event.get('eventData', {}).get('testing_system_1', [{}])[0].get('variationId')),
        'testing_system_1_variation_name': to_unquoted(event.get('eventData', {}).get('testing_system_1', [{}])[0].get('variationName')),
        # The testing_system_2 fields are usually in a dict, but occasionally are in a list like the testing_system_1 fields.
        'testing_system_2_application_id': unlist(event.get('eventData', {}).get('testing_system_2', {})).get('applicationId'),
        'testing_system_2_experiment_id': unlist(event.get('eventData', {}).get('testing_system_2', {})).get('experimentId'),
        'testing_system_2_experiment_name': unlist(event.get('eventData', {}).get('testing_system_2', {})).get('experimentName'),
        'testing_system_2_variation_id': unlist(event.get('eventData', {}).get('testing_system_2', {})).get('variationId'),
        'testing_system_2_variation_name': unlist(event.get('eventData', {}).get('testing_system_2', {})).get('variationName'),
        'referer': headers.get('referer'),
        'ua_browser_family': user_agent['ua_browser_family'],
        'ua_browser_version': user_agent['ua_browser_version'],
        'ua_os_family': user_agent['ua_os_family'],
        'ua_os_version': user_agent['ua_os_version'],
        'ua_device_family': user_agent['ua_device_family'],
        'ua_device_brand': user_agent['ua_device_brand'],
        'ua_device_model': user_agent['ua_device_model'],
        'ua_is_mobile': user_agent['ua_is_mobile'],
        'ua_is_tablet': user_agent['ua_is_tablet'],
        'ua_is_touch': user_agent['ua_is_touch'],
        'ua_is_pc': user_agent['ua_is_pc'],
        'ua_is_bot': user_agent['ua_is_bot'],
        'ua_is_lv': user_agent['ua_is_lv'],
        'user_agent': event.get('request').get('user-agent'),
        'x_real_ip': event.get('request').get('x-real-ip'),
        'utm_source': extract_external_cookie_3_dict(event).get('utm_source'),
        'utm_medium': extract_external_cookie_3_dict(event).get('utm_medium'),
        'utm_campaign': extract_external_cookie_3_dict(event).get('utm_campaign'),
        'utm_content': extract_external_cookie_3_dict(event).get('utm_content')
    }

    clean_map = {
        'event_id': (lambda v:
            sqs_string(v, 36, 'event_id', nullable=False)),
        'application_id': (lambda v:
            sqs_string(v, 26, 'application_id', nullable=False)),
        'user_hashed_id': (lambda v:
            sqs_string(v, 32, 'user_hashed_id', nullable=True)),
        'user_id': (lambda v:
            integer(v, 8, nullable=True, null_invalid=True)),
        'page': (lambda v:
            sqs_string(v, 5000, 'page', nullable=True)),
        'internal_cookie_1': (lambda v:
            sqs_string(v, 500, 'internal_cookie_1', nullable=True)),
        'internal_cookie_2': (lambda v:
            sqs_string(v, 500, 'internal_cookie_2', nullable=True)),
        'event_date': (lambda v:
            date(v, nullable=False, null_invalid=False)),
        'event_time': (lambda v:
            date(v, nullable=False, null_invalid=False)),
        'testing_system_1_experiment_id': (lambda v:
            sqs_string(v, 20, 'testing_system_1_experiment_id', nullable=True)),
        'testing_system_1_experiment_name': (lambda v:
            sqs_string(v, 128, 'testing_system_1_experiment_name', nullable=True)),
        'testing_system_1_variation_id': (lambda v:
            sqs_string(v, 20, 'testing_system_1_variation_id', nullable=True)),
        'testing_system_1_variation_name': (lambda v:
            sqs_string(v, 128, 'testing_system_1_variation_name', nullable=True)),
        'testing_system_2_application_id': (lambda v:
            sqs_string(v, 50, 'testing_system_2_application_id', nullable=True)),
        'testing_system_2_experiment_id': (lambda v:
            sqs_string(v, 50, 'testing_system_2_experiment_id', nullable=True)),
        'testing_system_2_experiment_name': (lambda v:
            sqs_string(v, 128, 'testing_system_2_experiment_name', nullable=True)),
        'testing_system_2_variation_id': (lambda v:
            sqs_string(v, 50, 'testing_system_2_variation_id', nullable=True)),
        'testing_system_2_variation_name': (lambda v:
            sqs_string(v, 128, 'testing_system_2_variation_name', nullable=True)),
        'referer': (lambda v:
            sqs_string(v, 500, 'referer', nullable=True)),
        'ua_browser_family': (lambda v:
            sqs_string(v, 50, 'ua_browser_family', nullable=True)),
        'ua_browser_version': (lambda v:
            sqs_string(v, 50, 'ua_browser_version', nullable=True)),
        'ua_os_family': (lambda v:
            sqs_string(v, 50, 'ua_os_family', nullable=True)),
        'ua_os_version': (lambda v:
            sqs_string(v, 50, 'ua_os_version', nullable=True)),
        'ua_device_family': (lambda v:
            sqs_string(v, 50, 'ua_device_family', nullable=True)),
        'ua_device_brand': (lambda v:
            sqs_string(v, 50, 'ua_device_brand', nullable=True)),
        'ua_device_model': (lambda v:
            sqs_string(v, 50, 'ua_device_model', nullable=True)),
        'ua_is_mobile': (lambda v:
            boolean(v, nullable=False)),
        'ua_is_tablet': (lambda v:
            boolean(v, nullable=False)),
        'ua_is_touch': (lambda v:
            boolean(v, nullable=False)),
        'ua_is_pc': (lambda v:
            boolean(v, nullable=False)),
        'ua_is_bot': (lambda v:
            boolean(v, nullable=False)),
        'ua_is_lv': (lambda v:
            boolean(v, nullable=False)),
        'user_agent': (lambda v:
            sqs_string(v, 500, 'user_agent', nullable=True)),
        'x_real_ip': (lambda v:
            sqs_string(v, 15, 'x_real_ip', nullable=True)),
        'utm_source': (lambda v:
            sqs_string(v, 100, 'utm_source', nullable=True)),
        'utm_medium': (lambda v:
            sqs_string(v, 100, 'utm_medium', nullable=True)),
        'utm_campaign': (lambda v:
            sqs_string(v, 100, 'utm_campaign', nullable=True)),
        'utm_content': (lambda v:
            sqs_string(v, 100, 'utm_content', nullable=True))
    }

    (cleaned, errors) = clean_fields(clean_map, record)

    if errors:
        return SQSFailure(record, errors, rop.message)
    else:
        event_time = cleaned['event_time']
        shut_off_day = datetime(2016, 9, 13, 4, tzinfo=UTCTZ)

        if event_time < shut_off_day:
            return SQSSuccess(cleaned, rop.message, table=Pageviews)
        else:
            return SQSUntracked(record=cleaned, message=rop.message)


def find_user_hash(event, headers):
    # Sequentially searches for userHash in the same places Mango does
    return (event.get('userHash') or
            headers.get('user-context') or
            event.get('eventData', {}).get('userHash') or
            event.get('eventData', {}).get('USER_HASHED_ID')) or None


def dt_to_ts(dt):
    """
    Convert the given local datetime instance to a POSIX (UTC) timestamp in
    milliseconds; however precision finer than 1 second will be lost.
    """
    # datetime.timetuple() does not support precision finer than 1 second.
    return timegm(dt.utctimetuple()) * 1000


def ts_to_dt(ts):
    """
    Convert the given POSIX (UTC) timestamp in milliseconds to a localized
    datetime in the timezone specified by TZ.
    """
    return datetime.fromtimestamp(ts / 1000, tz=TZ)


def load_pageviews_to_redshift():
    """
    Copy all the pageview records from the DynamoDB table into Redshift.
    """

    log.debug("Loading pageviews from DynamoDB to Redshift staging table")

    with etl.services.redshift() as redshift:
        redshift.connection.autocommit = False

        sql = """
        COPY page_view_table
        FROM 'dynamodb://{dynamo_table}'
        CREDENTIALS 'aws_access_key_id={key};aws_secret_access_key={secret}'
        READRATIO 200 EMPTYASNULL BLANKSASNULL TRUNCATECOLUMNS
        COMPUPDATE OFF STATUPDATE OFF
        TIMEFORMAT AS 'YYYY-MM-DD HH:MI:SS'
        """.format(
            dynamo_table=Pageviews.table_name,
            key=os.getenv('AWS_ACCESS_KEY_ID'),
            secret=os.getenv('AWS_SECRET_ACCESS_KEY')
        )

        log.debug("Redshift COPY SQL:\n" +
                  re.sub(r"CREDENTIALS '.*?'", "CREDENTIALS '...'", sql))

        redshift.execute(sql)
        redshift.connection.commit()
