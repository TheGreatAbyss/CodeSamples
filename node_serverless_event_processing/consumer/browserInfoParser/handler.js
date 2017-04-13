'use strict';

let Action = require('./action');
exports.handler = (event, context, callback) => {
    (new Action(event, context)).handle(callback);
};

