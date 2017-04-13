'use strict';

const AWS = require('aws-sdk');
const kinesis = new AWS.Kinesis();

const exceptions = require('../lib/exceptions')
const log = require('../lib/logger');
const retryingPromise = require('../lib/retrying-promise');


function toKinesisRecords(messages) {
    return messages.map((message) => {
        return {
            Data: JSON.stringify(message, null, 2),
            PartitionKey: Math.random().toString()
        }
    });
}

function putRecords(messages, streamName) {
    return retryingPromise((resolve, retry, reject) => {
        let records;
        try {
            records = toKinesisRecords(messages);
        } catch (err) {
            log.error("Error converting to Kinesis records");
            reject(err)
        }
        const params = {
            Records: records,
            StreamName: streamName
        };

        kinesis.putRecords(params, function (err, data) {
            if (err) {
                log.error("Error pushing records to Kinesis, all records failed");
                retry(err)
            } else {
                if (data.FailedRecordCount > 0) {
                    const errorMessage = "One or more records failed to be sent to Kinesis";
                    log.error(errorMessage);

                    let erredMessages = [];
                    data.Records.map((message, index) => {
                        if (message.ErrorCode) {
                            erredMessages.push(messages[index])
                        }
                    });
                    reject(new exceptions.PartialSuccess(errorMessage, erredMessages));
                } else {
                    resolve(data)
                }
            }
        });
    })
}

function putRecord(message, streamName) {
    return retryingPromise((resolve, retry, reject) => {
        let record;
        try {
            record = toKinesisRecords([message])[0];
        } catch (err) {
            log.error("Error converting to Kineses records");
            reject(err)
        }

        record.StreamName = streamName;

        kinesis.putRecord(record, function (err, data) {
            if (err) {
                log.error("Failed to insert single record:");
                log.error(err);
                retry(err)
            } else {
                resolve(data)
            }
        });
    })
}

module.exports = {
    putRecords: putRecords,
    putRecord: putRecord
};
