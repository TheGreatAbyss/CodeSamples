"use strict";

const chai = require('chai');
const mockAWSSinon = require('mock-aws-sinon');
const rewire = require('rewire');
const sinon = require('sinon');
const log = require('../../lib/logger');


const pubSub = rewire('../pubSub.js');
const should = chai.should();


describe('Test Pub Sub', function () {
    let toKinesisRecords;
    describe('Test toKinesisRecords', function () {
        before(function () {
            toKinesisRecords = pubSub.__get__('toKinesisRecords');
        });
        it('returns what was expected above', function () {
            const messages = [{"Bernie": 1}, {"Eric": 2}]
            const returnedMessages = toKinesisRecords(messages);
            returnedMessages[0].Data.should.equal('{\n  "Bernie": 1\n}');
            returnedMessages[1].Data.should.equal('{\n  "Eric": 2\n}');
            returnedMessages[0].PartitionKey.should.exist;
            returnedMessages[1].PartitionKey.should.exist;
        });
    });


    describe('Test toKinesesRecords Mixed Failures', function () {
        before(function () {
            mockAWSSinon('Kinesis', 'putRecords', function (params, cb) {
                log.debug('pretending to call putRecords');

                cb(null, {
                    FailedRecordCount: 2,
                    Records: [
                        {
                            SequenceNumber: 56,
                            ShardId: 2
                        },
                        {
                            ErrorCode: 56,
                            ErrorMessage: 'Failure'
                        },
                        {
                            SequenceNumber: 57,
                            ShardId: 2
                        },
                        {
                            ErrorCode: 56,
                            ErrorMessage: 'Failure'
                        }
                    ]
                });
            });
        });
        after(function () {
            mockAWSSinon('Kinesis', 'putRecords').restore();
        });
        it('returns the correct records that failed', function (done) {
            const messages = [{"Message1": 1}, {"Message2": 2}, {"Message3": 3}, {"Message4": 4}];
            pubSub.putRecords(messages)
                .then((data) => {
                    done("This should not be called")
                })
                .catch((err) => {
                    err.failedRecords.should.deep.equal([{"Message2": 2}, {"Message4": 4}]);
                    done();
                })
        })
    })
})
;
