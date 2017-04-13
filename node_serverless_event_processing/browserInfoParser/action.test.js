'use strict'

const sinon = require('sinon');
const chai = require('chai');
const sinonChai = require('sinon-chai');
const expect = chai.expect;
chai.use(sinonChai);

const Action = require('./action')
const exceptions = require('../../lib/exceptions');
const pubSub = require('../../services/pubSub');
const log = require('../../lib/logger');

describe('Test browserInfoParser Lambda', function () {
    describe('Test complete pipeline without errors', function () {
        const pretendRecords = [{data1: 1}, {data2: 2}, {data3: 3}, {data4: 4}];
        const pretendRecordsb64 = pretendRecords.map((record) => {
            return {
                kinesis: {data: new Buffer(JSON.stringify(record), 'utf8').toString('base64')}
            }
        });
        var putRecords;
        before(function () {
            putRecords = sinon.stub(pubSub, 'putRecords').returns(
                Promise.resolve('data'));
        });
        after(function () {
            putRecords.restore()
        });
        it('completes without errors', function () {
            let action = new Action(
                {
                    Records: pretendRecordsb64
                },
                {
                    functionVersion: 'testStage',
                    invokedFunctionArn: 'arn:aws:lambda:us-east-1:XXXXXXXX:function:browserInfoParser:test'
                }
            );
            return action.invoke().then((data) => {
                log.debug(data);
                expect(data).to.match(/Done with Pipeline/);
                expect(putRecords).to.have.been.calledWith(pretendRecords);
                expect(putRecords).to.have.been.calledOnce;
            });
        })
    })

    describe('Test complete pipeline without errors and modify data with empty object', function () {
        const pretendRecords = [
            {data1: 1, content: {request: {}}},
            {data2: 2, content: {request: {}}},
            {data3: 3, content: {request: {}}},
            {data4: 4, content: {request: {}}}
        ];
        const pretendRecordsb64 = pretendRecords.map((record) => {
            return {
                kinesis: {data: new Buffer(JSON.stringify(record), 'utf8').toString('base64')}
            }
        });
        const expectedRecords = [
            {data1: 1, content: {request: {}, parsedData: {cookies: {}, userAgent: {}}}},
            {data2: 2, content: {request: {}, parsedData: {cookies: {}, userAgent: {}}}},
            {data3: 3, content: {request: {}, parsedData: {cookies: {}, userAgent: {}}}},
            {data4: 4, content: {request: {}, parsedData: {cookies: {}, userAgent: {}}}},
        ];
        var putRecords;
        before(function () {
            putRecords = sinon.stub(pubSub, 'putRecords').returns(
                Promise.resolve('data'));
        });
        after(function () {
            putRecords.restore();
        });
        it('completes without errors and returns the correct records', function () {
            let action = new Action(
                {
                    Records: pretendRecordsb64
                },
                {
                    functionVersion: 'testStage',
                    invokedFunctionArn: 'arn:aws:lambda:us-east-1:XXXXXXXXXX:function:browserInfoParser:test'
                }
            );

            return action.invoke().then((data) => {
                log.debug(data);
                expect(data).to.match(/Done with Pipeline/);
                expect(putRecords).to.have.been.calledWith(expectedRecords);
                expect(putRecords).to.have.been.calledOnce;
            });
        })
    });

    describe('Test complete pipeline without errors and modify data with real userAgent and Cookie Data', function () {
        const pretendRecords = [
            {
                data1: 1,
                content: {
                    request: {
                        "cookie": "pretendCooke1=xxxxxxxxx; pretendCookie2=/some/page/; pretendCookie3=cookie_data; pretend_cooke4=cookie_data; parsedCookie1=cookie_data_to_be_parsed;  parsedCookie2=bkqYTV58;  __utma=utm_data; __utmb=utmb_data; __utmc=utmc_data; __utmz=utmc_data; ",
                        "referer": "https://referer/page/",
                        "userAgent": "Mozilla/5.0 (Linux; Android 6.0.1; SAMSUNG-SM-G935A Build/MMB29M; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/52.0.2743.98 Mobile Safari/537.36 [FB_IAB/FB4A;FBAV/93.0.0.13.69;]"
                    },
                }
            }
        ];
        const pretendRecordsb64 = pretendRecords.map((record) => {
            return {
                kinesis: {data: new Buffer(JSON.stringify(record), 'utf8').toString('base64')}
            }
        });
        const expectedRecords = [
            {
                data1: 1,
                content: {
                    request: {
                        "cookie": "pretendCooke1=xxxxxxxxx; pretendCookie2=/some/page/; pretendCookie3=cookie_data; pretend_cooke4=cookie_data; parsedCookie1=cookie_data_to_be_parsed;  parsedCookie2=bkqYTV58;  __utma=utm_data; __utmb=utmb_data; __utmc=utmc_data; __utmz=utmc_data; ",
                        "referer": "https://referer/page/",
                        "userAgent": "Mozilla/5.0 (Linux; Android 6.0.1; SAMSUNG-SM-G935A Build/MMB29M; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/52.0.2743.98 Mobile Safari/537.36 [FB_IAB/FB4A;FBAV/93.0.0.13.69;]"
                    },
                    parsedData: {
                        cookies: {
                            parsedCookie1: "parsed_cookie_data_1",
                            parsedCookie2: "parsed_cookie_data_2",
                            utm_campaign: "campaign_data",
                            utm_content: "utm_parsed_results",
                            utm_medium: "mobile",
                            utm_source: "some_advertising_source"
                        },
                        userAgent: {
                            ua_browser_family: "Other or Unknown",
                            ua_browser_version: "93.0.0",
                            ua_device_brand: undefined,
                            ua_device_family: "Samsung $2",
                            ua_device_model: undefined,
                            ua_is_bot: false,
                            ua_is_lv: false,
                            ua_is_mobile: true,
                            ua_is_pc: false,
                            ua_is_tablet: false,
                            ua_is_touch: true,
                            ua_os_family: "Android",
                            ua_os_version: "6.0.1"
                        }
                    }
                }
            }
        ];
        var putRecords;
        before(function () {
            putRecords = sinon.stub(pubSub, 'putRecords').returns(
                Promise.resolve('data'));
        });
        after(function () {
            pubSub.putRecords.restore()
        });
        it('completes without errors and returns the correctly parsed object', function () {
            let action = new Action(
                {
                    Records: pretendRecordsb64
                },
                {
                    functionVersion: 'testStage',
                    invokedFunctionArn: 'arn:aws:lambda:us-east-1:XXXXXXXXX:function:browserInfoParser:test'
                }
            );
            return action.invoke().then((data) => {
                log.debug(data);
                expect(data).to.match(/Done with Pipeline/);
                expect(putRecords).to.have.been.calledWith(expectedRecords);
                expect(putRecords).to.have.been.calledOnce;
            })
        })
    });

    describe('Test completes with putRecords error', function () {
        const pretendRecords = [{data1: 1}, {data2: 2}, {data3: 3}, {data4: 4}];
        const pretendRecordsb64 = pretendRecords.map((record) => {
            return {
                kinesis: {data: new Buffer(JSON.stringify(record), 'utf8').toString('base64')}
            }
        });
        var putRecords, putRecord;
        beforeEach(function () {
            putRecords = sinon.stub(pubSub, 'putRecords').returns(
                Promise.reject(new exceptions.PartialSuccess("One or more records failed to be sent to Kinesis", [{data1: 1},  {data3: 3}]))
            );
            putRecord = sinon.stub(pubSub, 'putRecord').returns(
                Promise.resolve("success")
            );
        });
        afterEach(function () {
            putRecords.restore();
            putRecord.restore();
        });
        it('completes by using putRecord', function () {
            let action = new Action(
                {
                    Records: pretendRecordsb64
                },
                {
                    functionVersion: 'testStage',
                    invokedFunctionArn: 'arn:aws:lambda:us-east-1:XXXXXXXXX:function:browserInfoParser:test'
                }
            );
            return action.invoke().then((data) => {
                log.debug(data);
                expect(data).to.match(/Done with Pipeline/);
                expect(putRecords).to.have.been.calledWith(pretendRecords);
                expect(putRecords).to.have.been.calledOnce;
                expect(putRecord).to.have.been.calledTwice;
                expect(putRecord).to.have.been.calledWithMatch(
                    sinon.match.has("data1").or(sinon.match.has("data2")));
            })
        })
    });

    describe('Test fails gracefully with error', function () {
        const pretendRecords = [{data1: 1}, {data2: 2}, {data3: 3}, {data4: 4}];
        const pretendRecordsb64 = pretendRecords.map((record) => {
            return {
                kinesis: {data: new Buffer(JSON.stringify(record), 'utf8').toString('base64')}
            }
        });
        var putRecords;
        beforeEach(function () {
            putRecords = sinon.stub(pubSub, 'putRecords').returns(
                Promise.reject("You got an error bro")
            );
        });
        afterEach(function () {
            putRecords.restore();
        });
        it('completes by using putRecord', function () {
            var action = new Action(
                {
                    Records: pretendRecordsb64
                },
                {
                    functionVersion: 'testStage',
                    invokedFunctionArn: 'arn:aws:lambda:us-east-1:XXXXXXXXX:function:browserInfoParser:test'
                }
            );
            return action.invoke().then((result) => {
                fail("putRecord() succeeded when it was expected to fail. Received result: " + result);
            }, (err) => {
                expect(err).to.match(/You got an error bro/);
                expect(putRecords).to.have.been.calledOnce;
                expect(putRecords).to.have.been.calledWith(pretendRecords);
            });
        })
    });
});
