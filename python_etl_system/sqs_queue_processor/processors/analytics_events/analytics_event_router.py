import os
import time
from collections import defaultdict
from boto.dynamodb2.exceptions import JSONResponseError

from conf.etl_config import THROUGHPUT_TABLE_FOR_SQS_PROCESSOR
from etl.celery import app
from etl.lib.dynamodb import map_dynamo_types, put_in_dynamo
from etl.lib.general import timed_job
from etl.lib.rop import compose
from etl.lib.vacuum import analyze_table
from etl.services.dynamodb import Pageviews, delete_table_wait, create_table_wait
from etl.services.logging import get_logger

from etl.sqs_queue_processor.lib.rop import SQSFailure, SQSSuccess, SQSUntracked
from etl.sqs_queue_processor.lib.delete_records import delete_records
from etl.sqs_queue_processor.lib.queue_and_json_operations import generate_queue_object, pull_json_event
from etl.sqs_queue_processor.processors.analytics_events.generic_ui_router import *
from etl.sqs_queue_processor.processors.analytics_events.page_views import load_pageviews_to_redshift

log = get_logger(__name__)


def route_on_event_name(rop):
    if not isinstance(rop, SQSSuccess):
        return rop

    event = rop.record
    try:
        event_name = event['eventName']
    except KeyError as e:
        error_message = 'No eventName field'
        log.error(error_message)
        return SQSFailure(record=event, create_error={error_message: e}, message=rop.message)

    if event_name == 'Generic Ui':
        new_rop = parse_generic_ui(rop)
        return new_rop
    else:
        return SQSUntracked(record=rop.record, message=rop.message)


def clean_and_insert_record_into_dynamodb(rop):
    if not isinstance(rop, SQSSuccess):
        return rop

    cleaned_rop = map_dynamo_types(rop)

    with rop.table.batch_write() as table_batch:
        final_rop = put_in_dynamo(cleaned_rop, table_batch)

    return final_rop


def create_tables():
    Pageviews.throughput = THROUGHPUT_TABLE_FOR_SQS_PROCESSOR
    create_table_wait(Pageviews)


def delete_tables():
    delete_table_wait(Pageviews)


def load_data():
    load_pageviews_to_redshift()


def load_leftover_data():
    try:
        status = Pageviews.describe()['Table']['TableStatus']
        if status == 'ACTIVE':
            log.warning("Existing PageView table found, loading leftover data")
            load_data()
    except JSONResponseError as e:
        if e.error_code == 'ResourceNotFoundException':
            log.debug("No existing PageView Table")
            pass


def analyze_tables():
    analyze_table(None, 'pageviews')


@app.task
@timed_job(log)
def pull_and_route(manual_run_time=0):
    queue = generate_queue_object(os.getenv('PRODUCTION_EVENTS_QUEUE'))

    if manual_run_time:
        run_minutes = int(manual_run_time)
    else:
        run_minutes = int(os.getenv('PRODUCTION_EVENTS_QUEUE_RUN_TIME', 60))

    load_leftover_data()
    delete_tables()
    create_tables()

    process_message = compose(
        pull_json_event,
        route_on_event_name,
        clean_and_insert_record_into_dynamodb,
    )

    succeeded = 0
    untracked = 0
    failures = defaultdict(int)

    log.debug("Pulling Messages Off Queue")

    test_count_set = set([])

    # Because this job is being run as a batch, I'm manually setting how long it should run
    # If we get to a point where we are not keeping up with the queue, the time needs to be increased
    end_time = time.time() + (60 * run_minutes)
    while time.time() < end_time:

        message_list = queue.receive_messages(MaxNumberOfMessages=10)

        # returned message_list should be a list containing up to 10 messages

        delete_dict_list = []
        id_to_event_id_dict = {}

        for index, message in enumerate(message_list):

            result = process_message(message)
            if isinstance(result, SQSSuccess):
                succeeded += 1
                test_count_set.add(result.record['event_id'])
                id_to_event_id_dict[str(index)] = result.record['event_id']

            if isinstance(result, SQSFailure):
                for error in result.errors.keys():
                    failures[error] += 1

            if isinstance(result, SQSUntracked):
                untracked += 1

            delete_dict_list.append({'Id': str(index), 'ReceiptHandle': message.receipt_handle})

        if delete_dict_list:
            delete_records(delete_dict_list, id_to_event_id_dict, queue)

    log.info("Succeeded: %d  Untracked: %d", succeeded, untracked)
    if len(failures) == 0:
        log.info("No Errors")
    else:
        for key, item in failures.items():
            log.info(key + ' generated following number of failures: ' + str(item))

    log.info("Actual number of records added to Dynamo : " + str(len(test_count_set)))

    load_data()
    delete_tables()
