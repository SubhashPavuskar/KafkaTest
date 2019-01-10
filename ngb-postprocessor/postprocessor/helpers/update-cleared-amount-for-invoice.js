/* jshint strict:true */
/* jshint node: true */
'use strict';

let config = require('config');
let promise = require("bluebird");
let kafka = promise.promisifyAll(require('kafka-node'));
let cassaClient = require('../cassandra');
const mysql = require('../mysql');
let logger = require('./logger');
let hexToUuid = require('hex-to-uuid');
let connectionString = `${config.get('kafka.host')}:${config.get('kafka.port')}`;
let rejectionNotification = config.get('kafka.rejectionNotification');
let Consumer = kafka.Consumer;
let client = new kafka.Client(connectionString + '/');
let Producer = new kafka.HighLevelProducer(client);
let isJSON = require('is-valid-json');
let updateClearedAmountForInvoiceTopic = config.get('kafka.updateClearedAmountForInvoice');
let updateClearedAmountForInvoiceConsumer = new Consumer(
    client,
    [],
    {fromOffset: true}
);

updateClearedAmountForInvoiceConsumer.addTopics([
    {topic: updateClearedAmountForInvoiceTopic, partition: 0, offset: 0}
], () => logger.info('topic ' + updateClearedAmountForInvoiceTopic + ' added to consumer for listening'));


function rejectionNotify(message) {
    logger.info("Sending rejection notification to kafka - ", message);
    Producer.send([{topic: rejectionNotification, messages: [JSON.stringify(message)]}], (err, data) => {
        if (err) {
            logger.info("\n Filed to send rejection topic to kakfa - request payload - ", message, "  error - ", err);
        } else {
            logger.error("\n Rejection notification sent to kafka for request ", message);
        }
    });
}

function updateClearedAmount(queryPayload, requestPayload, message){
    let billRunId;
    let getBillRunIdQuery = "select * from invoices where id = " + queryPayload.invoiceId + " and billing_account_id = " + queryPayload.billingAccountId;
    logger.info("Query to get bill run id - - ",getBillRunIdQuery);
    cassaClient.executeQuery(getBillRunIdQuery).then(function (data) {
        if(data.rowLength > 0 && data.rows[0].bill_run_id) {
            billRunId = data.rows[0].bill_run_id;
            logger.info("Got bill run id  - - ", billRunId);
            queryPayload.billRunId = billRunId;
            let query = "select * from "+queryPayload.tableName+" where id = " + queryPayload.id +" and billing_run_id = " + queryPayload.billRunId;
            logger.info("Checking records existing in the table or not for the query - - ", query);
            cassaClient.executeQuery(query).then(function (invoiceDetails) {
                if(data.rowLength > 0 && invoiceDetails.rows[0]) {
                    logger.info(requestPayload.identifierType + " Level Invoice details  ", invoiceDetails.rows[0]);
                    let updateQuery = "update "+queryPayload.tableName+" set cleared_amount = "+parseFloat(queryPayload.clearedAmount)+", date ='"+queryPayload.date+"' where id = "+ queryPayload.id +" and billing_run_id = " + queryPayload.billRunId;
                    logger.info("Query to updating cleared amount  - - ", updateQuery);
                    cassaClient.executeQuery(updateQuery).then(function (updateResponse) {
                        logger.info("Cleared amount and date is successfully -", updateResponse);
                    }, function (failedResponse) {
                        logger.error("Failed to update cleared amount request - >", message, " message-- ", failedResponse);
                        requestPayload.rejectionReason = "Failed to update cleared amount";
                        message.value = requestPayload;
                        rejectionNotify(message);
                    });
                } else {
                    logger.error("No data found in "+queryPayload.tableName+" -> request - ", JSON.stringify(requestPayload) + " error - > ", error);
                    requestPayload.rejectionReason = "No data found in "+queryPayload.tableName;
                    message.value = requestPayload;
                    rejectionNotify(message);
                }

            }, function (error) {
                logger.error("No data found for that invoice number and Id -> request - ", JSON.stringify(requestPayload) + " error - > ", error);
                requestPayload.rejectionReason = "No data found for that invoice number and Id";
                message.value = requestPayload;
                rejectionNotify(message);

            });
        } else {
            logger.info("No bill run id in invoice table  for the request - - ",requestPayload);
            let errorResponse = {};
            errorResponse.rejectionReason = "No bill run id in invoice table  for the request";
            message.value = errorResponse;
            rejectionNotify(message);
        }
    }, function () {
        logger.info("No bill run id in invoice table  for the request - - ",requestPayload);
        let errorResponse = {};
        errorResponse.rejectionReason = "No bill run id in invoice table  for the request";
        message.value = errorResponse;
        rejectionNotify(message);
    });

}

updateClearedAmountForInvoiceConsumer.on('message', function (message) {
    logger.info('\n GOT REQUEST TO UPDATE CLEARED AMOUNT FOR INVOICE -> ', message);
    if (isJSON(message.value)) {
        let requestPayload = JSON.parse(message.value);
        let invoiceId = requestPayload.id;
        let date = requestPayload.date;
        let clearedAmount = requestPayload.clearedAmount;
        let identifierType = requestPayload.identifierType;
        if (invoiceId && date && clearedAmount && (identifierType === 'Account' || identifierType === 'Service') && requestPayload.identifier) {
            let queryPayload = {
                id: requestPayload.identifier,
                invoiceId: invoiceId,
                billRunId: '',
                date : date,
                clearedAmount : parseInt(clearedAmount),
                billingAccountId : requestPayload.identifier,
                tableName: identifierType === 'Service' ? 'service_level_invoices' : 'account_level_invoices'
            };
          if(identifierType === 'Account'){
              updateClearedAmount(queryPayload, requestPayload, message);
          }

          if(identifierType === 'Service'){
              let serviceId= requestPayload.identifier.replace(/-/g,'');
              let getBillingAccountIdQuery = "select HEX(BILLING_ACCOUNT_ID) AS billingAccountId from SERVICE_ACCOUNTS where id = UNHEX('"+serviceId+"')";
              logger.info('Query to get Billing account id - ',getBillingAccountIdQuery);
              mysql.execute(getBillingAccountIdQuery).then((result) => {
                  logger.info("Got billing account ID from SERVICE_ACCOUNTS table -", result);
                  if(result && result.billingAccountId){
                      queryPayload.billingAccountId = hexToUuid(result.billingAccountId);
                      updateClearedAmount(queryPayload, requestPayload, message);
                  } else {
                      logger.error("Billing account not found in SERVICE_ACCOUNTS table for the request. - ", JSON.stringify(requestPayload));
                      requestPayload.rejectionReason = "Billing account not found in SERVICE_ACCOUNTS table for the request.";
                      message.value = requestPayload;
                      rejectionNotify(message);
                  }
              }, function () {
                  logger.error("Error billing account ID From SERVICE_ACCOUNTS table for the request. - ", JSON.stringify(requestPayload));
                  requestPayload.rejectionReason = "Billing account not found in SERVICE_ACCOUNTS table for the request.";
                  message.value = requestPayload;
                  rejectionNotify(message);
              });
          }

        } else {
            logger.error("Invalid request -> missing some mandatory fields -> request - ", JSON.stringify(requestPayload));
            requestPayload.rejectionReason = "Invalid request -> missing some mandatory fields.";
            message.value = requestPayload;
            rejectionNotify(message);
        }
    } else {
        logger.error("Invalid request -> Invalid Request JSON");
        let errorResponse = {};
        errorResponse.rejectionReason = "Invalid request -> Invalid JSON for update cleared amount";
        message.value = errorResponse;
        rejectionNotify(message);
    }
});