import boto3
import json

from etl.services.logging import get_logger
from etl.sqs_queue_processor.lib.rop import SQSFailure, SQSSuccess, SQSUntracked

log = get_logger(__name__)


def generate_queue_object(queue_string):
    sqs = boto3.resource('sqs', region_name='us-east-1')
    return sqs.get_queue_by_name(QueueName=queue_string)


def pull_json_event(message):

    # Test for message with no body
    try:
        message_body = message.body
    except AttributeError as e:
        error_message = 'Message did not have a body'
        log.error(error_message)
        return SQSFailure(record=message, create_error={error_message: e}, message=None)

    # convert message body into JSON object
    try:
        outer_message_json = json.loads(message_body)
    except json.JSONDecodeError as e:
        error_message = 'Could not convert outer message body to JSON'
        log.error(error_message)
        return SQSFailure(record=message, create_error={error_message: e}, message=None)

    # pull event off of outer_message_json
    try:
        event_string = outer_message_json['Message']
    except KeyError as e:
        error_message = 'Outer message JSON does not contain a "Message" field'
        log.error(error_message)
        return SQSFailure(record=message, create_error={error_message: e}, message=None)

    # convert event string into final event JSON and return it
    try:
        event_json = json.loads(event_string)
    except json.JSONDecodeError as e:
        error_message = 'Could not convert event to JSON'
        log.error(error_message)
        return SQSFailure(record=message, create_error={error_message: e}, message=None)

    return SQSSuccess(record=event_json, message=message)
