# A few code samples from a Python ETL System I contributed to

This folder contains some code samples from a Python ETL system in which I was a code contributor.

* The sqs_queue_processor folder contains code that extracts event data from an AWS SQS queue
and inserts it into Redshift.  It is coded in the Railway Oriented Programming paradigm.

* The event_job_template folder contains code that is part of a system that extracts event data from
Elastic Search and inserts it into Redshift.  The specific piece of code provided acts as an intermediary
between JSON schema files, and another python module that does the actual extracting and loading of the data.
The provided code sample is written in a pythonic functional style.  The code reads the JSON schema files as input,
and outputs functions to the other module that pulls the data from Elastic Search, and functions that extract
the results for each field.