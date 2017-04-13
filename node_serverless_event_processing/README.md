# AWS Lambda Serverless Node - Code Sample

This folder contains a sample of some of the code I wrote for a serverless event
processing system in which I was a contributor. This project relied solely on
serverless AWS SaaS products such as Lambda, Kinesis, SQS, S3, DynamoDb, and
CloudFormation.  It was developed using the [Serverless](https://serverless.com/) framework 0.5 beta version.

* The services folder contains code I wrote to put data on a Kinesis stream
in both batchs, and individually.

* The browserInfoParser folder contains code for an AWS lambda that is triggered by an
initial Kinesis stream.  The lambda parses some header data, then puts the resulting data on a
second Kinesis stream using the PubSub code in the first folder.

Both folders contain tests that I wrote using mocha, chai, sinon, mock-aws-sinon, and rewire