'use strict';
const config = require('config');
const shell = require('shelljs');
const _ = require('lodash');
const YAML = require('yamljs');
let promise = require("bluebird");

const connectionString = `${config.get('kafka.host')}:${config.get('kafka.port')}`;
const consumerConfig = config.get('kafka.billRunIdsConsumerConfig');
consumerConfig["metadata.broker.list"] = connectionString;
const Kafka = require('node-rdkafka');
const consumer = new Kafka.KafkaConsumer(consumerConfig);
const topicName = config.get('kafka.billRunIdsTopic');

//consumerConfig["debug"] = 'all';

const mysql = require('../mysql');
const cassaClient = require('../cassandra');
const logger = require('../helpers/logger');
const queue = require('../queue');
const tablesConfig = YAML.load('./config/tables.yml');


//logging debug messages, if debug is enabled
consumer.on('event.log', function(log) {
    logger.info(log);
});

//logging all errors
consumer.on('event.error', function(err) {
    logger.error('Error from consumer for topic - ', topicName, ' error -> ',err);
});

consumer.on('ready', function(arg) {
    logger.info('consumer ready. for topic - ', topicName, ' call back ->',  JSON.stringify(arg));
    consumer.subscribe([topicName]);
    //start consuming messages
    consumer.consume();
});


consumer.on('data', function(message) {
    logger.info("Message reached to post-processor for topic -  ",topicName ,'  Bill run id - > ',message.value.toString());
    //committing offsets every numMessages
    //consumer.commit(message);
    consumer.commitMessage(message)
    // Output the actual message contents
    let billRunId = message.value.toString();
    logger.info(`Received Billing Run with ID ${billRunId}`);
    let newTopicName = config.get('kafka.prefixForBillRunInvoicesTopics') + billRunId;
    let invoiceIds = [], notificationTopic, billRunType;

    // deleting billing run specific topic. If exists means that previous execution failed
    logger.info(`Deleting Kafka topic ${newTopicName} for Billing Run ${billRunId}`);
    let topicDelete = shell.exec(config.get("kafka.deleteTopicCommand") + newTopicName, {silent: true}).stdout;

    let table = _.find(tablesConfig.additionalTables, {name: 'BILLING_RUNS'});
    let columns = Object.keys(table.columns);
    let idQueryPart = table.binaryPrimaryKey ? billRunId.replace(/-/g, '') : "";
    let dynamicQuery = `SELECT ${columns.join(', ')} FROM ${table.name} WHERE HEX(${table.primaryKeyColumnName})= '${idQueryPart}'`;
    logger.info("Query to get bill run type => ", dynamicQuery);
    mysql.execute(dynamicQuery).then((result) => {
        billRunType = result.BILLING_RUN_TYPE;
        notificationTopic = config.get('kafka.notifyBillRunIdsTopic');
        return cassaClient.getInvoiceIdsByBillingRunId(billRunId, (n, row) => {
            invoiceIds.push(row.id.toString());
        });
    }).then((res) => {
        if (res.rowLength === 0) {
            let payload = JSON.stringify({
                billRunId: billRunId,
                invoicesCount: 0,
                topicName: newTopicName,
                tenantId: config.get('application.tenant'),
                billType: config.billingRunTypes[billRunType],
                invoiceSampleBatchID: ''
            });
            return queue.enqueue('notify', "notify", [billRunId, notificationTopic, payload]);
        } else {
            logger.info(`Billing Run with ID ${billRunId} contains ${res.rowLength} invoices. Adding them to decoration job queue`);
            let enqueuePromises = invoiceIds.map((invoiceId) => queue.enqueue('decorate', "decorate", [invoiceId, newTopicName, res.rowLength, config.billingRunTypes[billRunType], notificationTopic, '']));
            return promise.all(enqueuePromises);
        }
    }).then(() => {
        let table = tablesConfig.updateBillingRunStatus;
        let columns = Object.keys(table.columns);
        let idQueryPart = table.binaryPrimaryKey ? `${billRunId}` : `'${billRunId}'`;
        let updateQuery = "UPDATE  " + table.name + " SET " + columns + "= '" + table.columns.BILLING_RUN_STATUS + "' WHERE HEX(" + table.primaryKeyColumnName + ") ='" + idQueryPart.replace(/-/g, '').trim() + "'";
        logger.info("update query - - ", updateQuery);
        mysql.execute(updateQuery).then((result) => {
            logger.info("Status updated successfully.");
        });
    }).then(() => logger.info(`All invoices of Billing Run ${billRunId} have been added to queue for processing`))
        .catch((err) => logger.error(`Got Cassandra error for reading Invoice IDs for Billing Run ID ${billRunId} : ${err}`));
});

consumer.on('disconnected', function(arg) {
    console.log('consumer disconnected. ' + JSON.stringify(arg));
});

//starting the consumer
consumer.connect();

//stopping this example after 30s
//setTimeout(function() {
//  consumer.disconnect();
//}, 30000);
