/* jshint strict:true */
/* jshint node: true */

'use strict';
const promise = require("bluebird");
const async = require('async');
const config = require('config');
const cassaClient = require('./../cassandra');
const decorator = require('./../helpers/decorator');
const logger = require('./../helpers/logger');
const kafka = promise.promisifyAll(require('kafka-node'));
const connectionString = `${config.get('kafka.host')}:${config.get('kafka.port')}`;
const invoicesProducerClient = new kafka.Client(connectionString, `invoicesProducerClient-${process.pid}`);



const offset = new kafka.Offset(invoicesProducerClient);
const invoicesProducer = new kafka.HighLevelProducer(invoicesProducerClient);

function send(message, topic) {
  return new promise((resolve, reject) => {
    return invoicesProducer.send([{topic: topic, messages: [message]}], (err, data) => {
      if(err) reject(err);
      else resolve(data);
    });
  });
}

module.exports.notify = function(billingRunId, notificationTopic, payload) {
    return send(payload, notificationTopic).then((message) => logger.info(`Added Billing Run ${billingRunId} to integration topic`));
};
