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
let produces = require('../kafka-produces/producer');

module.exports.decorate = function(invoiceId, newTopicName, totalNumberOfInvoices, billRunType, notificationTopic, invoiceSampleBatchID) {
    logger.info("Request came to decorate job - with invoice id is ", invoiceId, " Dynamic topic name is", newTopicName);
    return cassaClient.getInvoiceById(invoiceId)
        .then((invoice) => {
            logger.info("Invoice details for invoice id -", invoiceId);
            if (invoice) {
                const invoiceJson = JSON.parse(invoice.json);
                let billRunId = invoiceJson.metadata.billingRunId;
                let billingAccountId = invoiceJson.metadata.billingAccountId;
                let invoiceStatusSummary = JSON.stringify({
                    invoiceStatus: invoiceJson.invoiceSummary.invoiceStatusAtCommit,
                    toBePaid: invoiceJson.invoiceSummary.toBePaid,
                    alreadyPaid: "0.00",
                    invoiceDate: invoiceJson.invoiceSummary.invoiceDate,
                    invoiceNumber: invoiceJson.invoiceSummary.invoiceNumber,
                    invoiceCurrency: invoiceJson.billingAccountDetails.invoiceCurrency.code
                });
                invoiceJson.metadata.billingRunType = billRunType;
                return decorator.getDecoratedInvoice(invoiceJson)
                    .then((finalInvoice) => {
                        if(billRunType !== 'Sample') {
                            return cassaClient.insertInvoice(invoiceId, billingAccountId, billRunId, finalInvoice, invoiceStatusSummary);
                        } else {
                            return  finalInvoice;
                        }
                    })
                    .then((finalInvoice) => {
                        logger.info("Sending final invoice to kafka to the topic  ",newTopicName);
                        return produces.sendMessage(newTopicName, finalInvoice, totalNumberOfInvoices);
                    })
                    .then((message) => {
                        console.log("message - - ",message);
                        if (message.offset && message.offset === totalNumberOfInvoices-1) {
                            logger.info(`All invoices of Billing Run ${billRunId} have been processed`);

                            let payload = JSON.stringify({
                                billRunId: billRunId,
                                invoicesCount: totalNumberOfInvoices,
                                topicName: newTopicName,
                                tenantId: config.get('application.tenant'),
                                billType: billRunType,
                                invoiceSampleBatchID: invoiceSampleBatchID || ''
                            });
                            return produces.sendMessage(notificationTopic, payload).then((message) => logger.info(`Added Billing Run ${billRunId} to integration topic`));
                        }
                        else
                            return;
                    })
                    .catch((err) => logger.error(err));
            }
            else {
                logger.error(`Can not find invoice with id ${invoiceId} in Cassandra`);
            }
        })
        .catch((err) => {
            logger.error(`ERROR for invoice with ID ${invoiceId}: ${err}`);
        });
};
