from etl.sqs_queue_processor.lib.rop import *

from etl.sqs_queue_processor.processors.analytics_events.page_views import handle_page_view


def parse_generic_ui(rop):
    event = rop.record

    event_type = event.get('eventType')
    if event_type in ['pageView', 'pageLoad']:
        return handle_page_view(rop)
    else:
        return SQSUntracked(record=rop.record, message=rop.message)
