# A few code samples from a Python ETL system I contributed to

This folder contains some code samples from a Python ETL system in which I was a code contributor.

# SQS Queue Event Processor

This folder contains code that extracts event data from an AWS SQS queue and inserts it into Redshift.
The example event included is page view event data.  The code uses a railway oriented programming
(ROP) paradigm where the result of each function is encapsulated in an ROP abstract class,
whose instantiated subclasses are either a success or a failure object (similar to a monad in other
more functional languages).  Since the functions all take the same ROP class of objects as inputs,
they are composed together into a pipeline function where the output of one function is the
input of the next.  Failure objects are passed along the pipeline and collected at the end
to be logged out to standard error (which gets collected into an external logging system).
SQS messages belonging to the failure objects are not deleted from the queue

As event data is pulled off the queue it is stored in DynamoDb where the final step is to issue
a copy statement from the Redshift table against the DynamoDb table.

#### The general steps of the process are:

1. Issue a copy statement against any errant DyanmoDb data that was not successfully processed on the
last run
#### For each event:
1. Pull event data off the queue.
1. Determine the event type.
1. Parse the event data by pulling out relevant fields, and parsing out cookie data from the headers.
1. Validate the returned data.
1. Insert the data into DyanmoDb
1. If a Success ROP object then delete the message off the queue.
#### On completion of all pulled data
1. Log the results and the number of errant events
1. Issue a copy command against the DynamoDb table.