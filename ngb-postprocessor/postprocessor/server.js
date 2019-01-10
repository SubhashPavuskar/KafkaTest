/* jshint strict:true */
/* jshint node: true */

'use strict';

const Hapi = require('hapi');
const Joi = require('joi');
const decorator = require('./helpers/decorator');
const cassaClient = require('./cassandra');
const server = new Hapi.Server();
const Boom = require('boom');
const logger = require('./helpers/logger');
const cache = require('./helpers/cache');
const kafkaPubSub = require('./kafka_pubsub');
require('./helpers/payment-debit-credit');
//const worker = require('./worker');


const config = require('config');
const promise = require("bluebird");
const connectionString = `${config.get('kafka.host')}:${config.get('kafka.port')}`;
const kafka = promise.promisifyAll(require('kafka-node'));
var client = new kafka.Client(connectionString + '/');
const kafkaTopicsconfig = require('./config/kafka-topics.json');

server.connection({
  port: "1992"//"%NODE_PORT%"
});

server.route([
  {
    method: 'PUT',
    path: '/invoices',
    config: {
      validate: {
        payload: Joi.any().required()
      }
    },
    handler: function (request, reply) {
      logger.info(`Got a PUT request to /invoices`);
      decorator.getDecoratedInvoice(request.payload).then((finalInvoice) => {
        logger.info(`PUT request to /invoices has been successfully processed`);
        reply(finalInvoice).type('application/json');
      });
    }
  },
  {
    method: 'GET',
    path: '/invoices/{billRunId}/{billingAccountId}',
    config: {
      validate: {
        params: {
          billRunId: Joi.string().guid().required(),
          billingAccountId: Joi.string().guid().required()
        }
      }
    },
    handler: function (request, reply) {
      const billRunId = request.params.billRunId;
      const billingAccountId = request.params.billingAccountId;

      logger.info(`Got a GET request to /invoices/${billRunId}/${billingAccountId}`);

      cassaClient.getInvoiceByBillingAccountIdAndBillRunId(billRunId, billingAccountId).then((rawInvoiceObject) => {
        if (rawInvoiceObject) {
          decorator.getDecoratedInvoice(JSON.parse(rawInvoiceObject.json)).then((finalInvoice) => {
            logger.info(`GET request to /invoices/${billRunId}/${billingAccountId} has been successfully processed`);
            reply(finalInvoice).type('application/json');
          });
        }
        else {
          logger.error(`Invoice with billRunId: ${billRunId} and billingAccountId: ${billingAccountId} can not be found`);
          reply(Boom.notFound(`Invoice with billRunId: ${billRunId} and billingAccountId: ${billingAccountId} can not be found`));
        }
      });
    }
  }
]);

server.start((err) => {
  if (err) {
    logger.error(err);
    throw err;
  }
  logger.info('Postprocessor running at::' + server.info.uri);
/*  let kafkaTopics = kafkaTopicsconfig.kafkaTopics;
  client.createTopics(kafkaTopics, (error, result) => {
    if(!error) {
      logger.info("Topic created at server start up:: ", result);
    } else {
        logger.info("Error Createing kafka topic at server statup", error);
    }
  });*/
});
