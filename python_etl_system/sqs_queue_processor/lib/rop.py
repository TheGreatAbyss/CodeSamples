from etl.lib.rop import TwoTrack, Success, Failure


class SQSSuccess(Success):
    """
    A value on the "success" track. Encapsulates the whole data record being
    processed.
    """
    def __init__(self, record, message, table=None):
        super(SQSSuccess, self).__init__(record)
        self.message = message
        self.table = table


class SQSFailure(Failure):
    """
    A value on the "failure" track. Keeps a dictionary of all the errors for a
    single failed record.
    """

    def __init__(self, record, create_error, message):
        super(SQSFailure, self).__init__(record, {})
        self.message = message
        self.errors = {}
        self.errors.update(create_error)


class SQSUntracked(TwoTrack):
    """
    A value on the "success" track. Encapsulates the whole data record being
    processed.
    """
    def __init__(self, record, message):
        self.record = record
        self.message = message
