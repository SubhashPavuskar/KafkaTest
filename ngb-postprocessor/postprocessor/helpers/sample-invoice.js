/* jshint strict:true */
/* jshint node: true */
'use strict';

const config = require('config');
const promise = require("bluebird");
const kafka = promise.promisifyAll(require('kafka-node'));
const cassaClient = require('../cassandra');
const logger = require('../helpers/logger');
const shell = require('shelljs');
const _ = require('lodash');
const queue = require('../queue');
const connectionString = `${config.get('kafka.host')}:${config.get('kafka.port')}`;
const consumerConfig = config.get('kafka.billRunIdsConsumerConfig');
consumerConfig.host = connectionString;
let isJSON = require('is-valid-json');
let Consumer = kafka.Consumer;
let client = new kafka.Client(connectionString + '/');
let sampleBillRunIdTopic =  config.get('kafka.sampleBillRunIdTopic');
let sampleBillRunConsumer = new Consumer(
    client,
    [],
    {fromOffset: true}
);

sampleBillRunConsumer.addTopics([
    {topic: sampleBillRunIdTopic, partition: 0, offset: 0}
], () => logger.info('topic ' + sampleBillRunIdTopic + ' added to consumer for listening'));

sampleBillRunConsumer.on('message', function(message) {
    logger.info('Got request to get sample bills -> ',message);
    if(isJSON(message.value) ) {
        let requetedData = JSON.parse(message.value);

        let newTopicName = config.get('kafka.prefixForSampleBillRunInvoicesTopics') + requetedData.sampleBatchId;
        logger.info(`Received Billing Run with ID ${requetedData.billRunId} and sample patch ID ${requetedData.sampleBatchId}`);

        // deleting billing run specific topic. If exists means that previous execution failed
        logger.info(`Deleting Kafka topic ${newTopicName} for sample patch id Run ${requetedData.sampleBatchId}`);
        let topicDelete = shell.exec(config.get("kafka.deleteTopicCommand") + newTopicName, {silent: true}).stdout;
        logger.info(topicDelete);

        logger.info(`Creating Kafka topic ${newTopicName} for Smaple patch id Run ${requetedData.sampleBatchId}`);
        client.createTopics(newTopicName, (error, result) => {
            logger.info("Dynamic topic is created -- ", result);
            if (!error) {
                let notificationTopic = config.get('kafka.notifyBillRunIdsTopic');
                cassaClient.getInvoiceNumberFromSampleBillRun(requetedData).then(function (responseData) {
                    logger.info("Invoice numbers - ",responseData);
                    if(Array.isArray(responseData)){
                        let enqueuePromises = responseData.map((invoiceId) => {
                            logger.info('Caliing decorator with invoice number  - > ', invoiceId.id);
                            return queue.enqueue('decorate', "decorate", [invoiceId.id, newTopicName, responseData.length, "Sample", notificationTopic, requetedData.sampleBatchId]);
                        });
                        return promise.all(enqueuePromises);
                    } else {
                        let payload = JSON.stringify({
                            billRunId: requetedData.billRunId,
                            invoicesCount: 0,
                            topicName: newTopicName,
                            tenantId: config.get('application.tenant'),
                            billType: "Sample",
							invoiceSampleBatchID: requetedData.sampleBatchId
                        });
                        return queue.enqueue('notify', "notify", [requetedData.billRunId, notificationTopic, payload]);
                    }
                })
                    .then(() => sampleBillRunConsumer.commitAsync())
                    .then(() => logger.info(`All sample invoices of Billing Run ${requetedData.billRunId} have been added to queue for processing`))
                    .catch((err) => logger.error(`Got Cassandra error for reading Invoice IDs for Billing Run ID ${requetedData.billRunId} : ${err}`));
            } else {
                logger.info("Failed to create dynamic topic-- ", error);
            }
        });
    } else {
        logger.info("Invalid JSON request. for request - - ", message);
    }
});
