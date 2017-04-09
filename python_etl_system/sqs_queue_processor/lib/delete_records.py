from etl.services.logging import get_logger
log = get_logger(__name__)


def delete_records(delete_dict_list, id_to_event_id_dict, queue):
    response = queue.delete_messages(Entries=delete_dict_list)
    _process_response(response, delete_dict_list, id_to_event_id_dict)


# This is a separate function purely for testing purposes.  There is a corresponding file test_delete_records
# that replicates a response and tests it
def _process_response(response, delete_dict_list, id_to_event_id_dict):
    if response['ResponseMetadata']['HTTPStatusCode'] != 200:
        log.warning("SQS batch delete unsuccessful, duplicate records may be added to Redshift",
                    extra={"event_ids": str(list(id_to_event_id_dict.values()))})

        return str(list(id_to_event_id_dict.values()))

    elif len(delete_dict_list) != len(response.get('Successful', [])):

        failed = []

        for item in response['Failed']:
            index = item['Id']
            event_id = id_to_event_id_dict.get(index)

            if event_id:
                failed.append(event_id)

        log.warning("SQS delete unsuccessful for {0} records. "
                    "duplicate records may be added to Redshift".format(len(failed)),
                    extra={"event_ids": str(failed)})

        return str(failed)

    else:
        return "SUCCESS"
