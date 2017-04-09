from etl.sqs_queue_processor.processors.common.validation import sqs_string


def test_good_str_length():
    test_str = "Bernie barks too much"
    test_length = 50

    result = sqs_string(test_str, test_length, "Dog", nullable=True)

    assert(result == test_str)


def test_bad_str_length():
    test_str = "Bernie barks too much"
    test_length = 10

    result = sqs_string(test_str, test_length, "Dog", nullable=True)

    assert(result == "Bernie bar")

