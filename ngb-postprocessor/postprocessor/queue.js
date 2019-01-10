/* jshint strict:true */
/* jshint node: true */
'use strict';

const jobs = require('./jobs');
const config = require('config');
const NR = require("node-resque");
const promise = require("bluebird");
const logger = require('./helpers/logger');

const queue = new NR.queue({connection: config.get('redis')}, jobs);
queue.on('error', (err) => logger.error(err));
queue.connect(() => logger.info("Postprocessor Master connected to Redis"));

module.exports.enqueue = function (que, job, args) {
    return new promise((resolve, reject) => {
        queue.enqueue(que, job, args, (err, result) => {
            console.log("queue error - ", err);
            console.log("Pushing tasks to queue -  Result - > ", result);
            resolve();

        });
    });
};
