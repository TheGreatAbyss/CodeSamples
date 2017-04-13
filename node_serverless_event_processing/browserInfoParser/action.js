/*
 * Parses user and browser info from HTTP headers into discrete fields
 */

'use strict';

const async = require('async');

const ConfiguredAction = require('../../lib/configured_action');
const exceptions = require('../../lib/exceptions');
const log = require('../../lib/logger');
const requestHeaderParser = require('../../lib/requestHeaderParser');
const pubSub = require('../../services/pubSub');

class Action extends ConfiguredAction {
    constructor(event, context) {
        super(event, context);
        if (event.Records) {
            this.records = event.Records;
        }
    }

    handle(callback) {
        this.logInvocation()
            .then(() => {
                return this.records.map((record) => {
                    let payload = JSON.parse(new Buffer(record.kinesis.data, 'base64').toString('utf8'));
                    if (payload.content && payload.content.request) {
                        payload.content.parsedData = {
                            userAgent: requestHeaderParser.parseUserAgent(payload.content.request.userAgent),
                            cookies: requestHeaderParser.parseCookies(payload.content.request.cookie)
                        };
                    }
                    return payload
                });
            })
            .then((transformedRecords) => {
                log.info("Putting Transformed Records On the OutPut Stream");
                log.debug(transformedRecords);
                return pubSub.putRecords(transformedRecords, process.env.OUTPUT_STREAM);
            })
            .then((data) => {
                log.info("Successful completion of lambda");
                callback(null, "Done with Pipeline")
            })
            .catch((err) => {
                if (err instanceof exceptions.PartialSuccess) {
                    log.info("Some records unsuccessfully placed on stream, retrying individually");
                    async.map(err.failedRecords, (record, cb) => {
                         pubSub.putRecord(record, process.env.OUTPUT_STREAM)
                             .then((success) => {cb(null, success)})
                             .catch((err) => {cb(err)})
                    }, (mapError, results) => {
                        if (mapError) {
                            log.error("Could not send all records to the OutPut Stream, failing the lambda");
                            log.error(mapError);
                            callback(mapError)
                        } else {
                            log.info("Succesfully sent all records to the OutPut Stream");
                            log.info(results);
                            callback(null, "Done with Pipeline")
                        }
                    })
                } else {
                    log.error("Failing lambda due to error");
                    log.error(err);
                    callback(err)
                }
            })
    }

}

module.exports = Action;
