/* jshint strict:true */
/* jshint node: true */

'use strict';

const config = require('config');
const promise = require("bluebird");
const cassandra = promise.promisifyAll(require('cassandra-driver'));
const logger = require('./helpers/logger');
const hexToUuid = require('hex-to-uuid');
const _util=require('./helpers/utility');
let dbConfig = Object.assign({}, config.get('cassandra'));
if (typeof dbConfig.contactPoints === 'string') {
    dbConfig.contactPoints = dbConfig.contactPoints.split(",").map((_) => _.trim());
}
const _ = require('lodash');
const cassaClient = new cassandra.Client(dbConfig);

cassaClient.connectAsync()
    .then(logger.info("Connected to Cassandra"))
    .catch((err) => {
        logger.error(`Error connecting to Cassandra Hosts ${dbConfig.contactPoints}: ${err}.`);
        setTimeout(() => {
            logger.error("Couldn't connect to Cassandra so shutting down. Supervisor will start me again.");
            process.exit();
        }, 3000);
    });

function getInvoiceById(id) {
    const query = 'SELECT * FROM invoices WHERE id=?';

    return cassaClient.executeAsync(query, [id], {prepare: true})
        .then((result) => result.rows[0])
        .catch((err) => logger.error(`Got Cassandra error for reading Invoice with ID ${id}: ${err}`));
}

function executeQuery(query){
    return new Promise(function (resolve, reject) {
        cassaClient.execute(query, [], function (err, result) {
            if (!err) {
                logger.info("Successfully executed casendra query - > ",query);
                resolve(result);
            } else {
                logger.info("No data found for query in casendra - >  ",query);
                reject([]);
            }
        });
    });
}
function getInvoiceByBillingAccountIdAndBillRunId(billRunId, billingAccountId) {
    const query = 'SELECT * FROM invoices WHERE billing_account_id=? AND bill_run_id=?';

    return cassaClient.executeAsync(query, [billingAccountId, billRunId], {prepare: true})
        .then((result) => result.rows[0])
        .catch((err) => logger.error(`Got Cassandra error for reading Invoice with Billing Run ID ${billRunId} and billingAccountId ${billingAccountId} : ${err}`));
}

function insertInvoice(id, billingAccountId, billRunId, invoiceJson, invoiceStatusSummary) {
    const query = 'INSERT INTO invoices_archive (id, billing_account_id, bill_run_id, json, invoice_status_summary) VALUES (?, ?, ?, ?, ?)';

    return cassaClient.executeAsync(query, [id, billingAccountId, billRunId, invoiceJson, invoiceStatusSummary], {prepare: true})
        .then(_ => invoiceJson)
        .catch((err) => logger.error(`Got Cassandra error for inserting Invoice with ID ${id}: ${err}`));
}

function getInvoiceIdsByBillingRunId(billRunId, cb) {
    const query = 'SELECT id FROM invoices_by_billing_run WHERE bill_run_id=?';

    return cassaClient.eachRowAsync(query, [billRunId], {autoPage: true, prepare: true}, cb)
        .catch((err) => logger.error(`Got Cassandra error for reading Invoice IDs for Billing Run ID ${billRunId} : ${err}`));
}

function updateInvoiceStatus(status, path, billRunId, invoiceSampleID, billAccountId, cb) {
    const query = 'UPDATE invoice_samples_by_billing_run set status= ? , path = ? WHERE billingrunid = ? AND invoicesamplebatchid= ? AND billingaccountid = ?';
    return cassaClient.executeAsync(query, [status, path, billRunId, invoiceSampleID, billAccountId], {prepare: true}, cb)
        .then(result => logger.info(`Result for updating status and path for Billing Run ID ${billRunId} : ${result}`))
        .catch((err) => logger.error(`Got Cassandra error for reading Invoice IDs for Billing Run ID ${billRunId} : ${err}`));
}

function getInvoiceNumberFromSampleBillRun(payloadData) {
    logger.info('Geting invoice id function - ', payloadData);
    return new Promise(function (resolve , reject){
        const query = 'select billingaccountid from invoice_samples_by_billing_run where billingrunid = '+payloadData.billRunId+' AND invoicesamplebatchid = '+payloadData.sampleBatchId;
        logger.info('Query to get bill run id from invoice_sample_by_billing_run table - >',query);
        let billingAccountId = '';
        cassaClient.execute(query, [], function (err, result) {
            if (!err) {
              _.forEach(result.rows, function (billingAccount) {
                  logger.info('After query bill run ID is -> ', billingAccount.billingaccountid);
                  billingAccountId += billingAccount.billingaccountid +',';
              });
              billingAccountId = billingAccountId.substr(0,billingAccountId.length - 1 );
                const getInvoiceNumberQuery = 'select id from invoices WHERE  billing_account_id IN('+billingAccountId+') and bill_run_id='+payloadData.billRunId;
                logger.info('Query to get invoice id from invoice table - >', getInvoiceNumberQuery);
                cassaClient.execute(getInvoiceNumberQuery, [], function (err, result) {
                    if(!err){
                        resolve(result.rows);
                    } else {
                        logger.info("Failed to get invoice id for the query -> ",getInvoiceNumberQuery);
                        reject('No data found in invoice  table');
                    }
                });

            } else {
                logger.info("Error while  getting data from invoice_samples_by_billing_run -> query - >  ",query);
                reject('No data found in invoice_sample_by_billing_run table');
            }
        });
    });
}

function checkPaymentsCreditDebitDetails(payloadData) {
    const query = "SELECT * from payments_credits_debits where identifier = '"+payloadData.identifier+"' and id='"+payloadData.id+"' ALLOW FILTERING";
    logger.info("Check records from payment_credits_debits table -> query -> ",query);
    return cassaClient.executeAsync(query, [], {prepare: true})
        .then((result) => {return result;})
        .catch((err) => logger.error(`Got Cassandra error for reading Invoice with payments_credits_debits`,err));
}

function insertPaymentsCreditsDebits(payloadData) {
    return new Promise(function(resolve,reject){
        let _PAMT=parseFloat(_util._exNU(payloadData.amount));
        let _PCAMT=parseFloat(_util._exNU(payloadData.clearedAmt));
        const query = "INSERT INTO payments_credits_debits (id, date,cleared_amount, amount, currency, transactionType, transaction, identifierType, identifier, additionalInfo, action,insertion_id) VALUES ('"+payloadData.id+"','"+payloadData.date+"',"+_PCAMT+","+_PAMT+",'"+payloadData.currency+"','"+payloadData.transactionType+"','"+payloadData.transaction+"','"+payloadData.identifierType+"','"+payloadData.identifier+"','"+payloadData.additionalInfo+"','"+payloadData.action+"',now())";
        logger.info("Inserting records in payment_credits_debits -> query -> ",query);
        return cassaClient.execute(query, [], function(err, result){
            if(!err){
                resolve({'response':{'code':'0','desc':'Record inserted successfully in payments_credits_debits table'}});
            }else{
                reject({'response':{'code':'-1','desc':'Failed to insert records into payments_credits_debits table'}});
            }
        });
    });
}


function deleteRecordPaymentsCreditsDebits(payloadData) {
    return new Promise(function(resolve,reject){
        let _typeID = payloadData.id;
        let _deleteRecord = "";
        let _findExist = "select insertion_id,id from payments_credits_debits where identifier = '"+payloadData.identifier+"' ALLOW FILTERING";
        logger.info("Checking records are exist or not for before deleting payments_credits_debits -> query - > ",_findExist);
        cassaClient.execute(_findExist, [], function(err, result){
            logger.info("Number of record found to delete-",result.rowLength);
            let _insertion_ids = [];
            let _ids = [];
            for(let i in result.rows) {
                _insertion_ids.push(result.rows[i].insertion_id);
                _ids.push("'"+result.rows[i].id+"'");
            }

            //deleting all records based on identifier from kafka request.
            if(_typeID === null || _typeID.toUpperCase() === "NULL" || _typeID ===""){
                logger.info("Delete all records");
                _deleteRecord = "DELETE  FROM payments_credits_debits WHERE insertion_id IN("+_insertion_ids+") AND id IN("+_ids+")  AND identifier = '"+payloadData.identifier+"'";
            }else{
                logger.error("Delete perticular records");
                _deleteRecord = "DELETE  FROM payments_credits_debits WHERE insertion_id IN("+_insertion_ids+") AND id ='"+_typeID+"'  AND identifier = '"+payloadData.identifier+"'";

            }
            logger.info("delete query", _deleteRecord);
            return cassaClient.execute(_deleteRecord, [], function(err, result){
                if(!err){
                    resolve({'repsonse':{'code':'0','desc':'Record deleted successfully from payments_credits_debits'}});
                }else{
                    reject({'repsonse':{'code':'-1','desc':'Failed to delete record from payments_credits_debits'}});
                }
            });
        });
    });
}
function insertIntoTransactionSummary(payload,billingCycleInstnceId,billingAccountID) {
    return new Promise(function (resolve , reject){
        let billAccId = billingAccountID.ID;
        let billInstId = billingCycleInstnceId.BILLING_CYCLE_INSTANCES_ID;

        const uuidBillInstId= hexToUuid(billInstId);
        const uuidBillAccId= hexToUuid(billAccId);
        const query = "select * from transaction_summary where id = "+uuidBillAccId+" AND billing_cycle_instance_id = "+uuidBillInstId;
        logger.info('Query to get data from transaction_summary table - >',query);

        checkTransactionSummaryDetails(uuidBillAccId,uuidBillInstId).then(function (responseData) {
            let transactionSummaryLength = responseData.rowLength;
            let transactionSummarydata = responseData;
            logger.info("Number of rows present in cassandra with the ID requested-------"+transactionSummaryLength);
            if(transactionSummaryLength === 0 ){
                logger.info("-------inserting fresh record in transaction_summary----------");
                insertDataIntoTransactionSummary(payload,uuidBillInstId,uuidBillAccId);
            }else if(transactionSummaryLength > 0 ){
                logger.info("-------Updating  existing record in transaction_summary----------");
                let dbPayload = transactionSummarydata.rows[0];
                updateDataIntoTransactionSummary(payload,uuidBillInstId,uuidBillAccId,dbPayload);
            }
        }, function(err){
            //Need Send notification
            logger.error("Error processing the request to cassandra ",err);
            let reason = 'Error processing the request to cassandra';
            callNotification(payload, reason);
        });

    });
}

function checkTransactionSummaryDetails(uuidBillAccId, uuidBillInstId) {
    const query = "select * from transaction_summary where id = ? AND billing_cycle_instance_id = ?";
    return cassaClient.executeAsync(query, [uuidBillAccId,uuidBillInstId], {prepare: true})
        .then((result) => {return result;})
        .catch((err) => logger.error(`Got Cassandra error for reading with transaction_summary`,err));
}


function insertDataIntoTransactionSummary(payload,uuidBillInstId,uuidBillAccId) {
    return new Promise(function(resolve,reject){
       logger.info('Request came to insertDataIntoTransactionSummary function -> payload -> ',payload);
        var transactionType = payload.transaction;
        let amount = parseFloat(_util._exNU(payload.amount));
        let queryIs = "INSERT INTO transaction_summary (id,billing_cycle_instance_id,credit,debit,due_amt,invoice,payment) VALUES (";
        if(transactionType){
            if(transactionType.toString().toUpperCase() === 'PAYMENT'){
                queryIs =  queryIs + uuidBillAccId+","+uuidBillInstId+",0,0,0,0,"+amount+")";
            } else if(transactionType.toString().toUpperCase() === 'CREDIT'){
                queryIs =  queryIs + uuidBillAccId+","+uuidBillInstId+","+amount+",0,0,0,0)";
            } else if(transactionType.toString().toUpperCase() === 'DEBIT'){
                queryIs =  queryIs + uuidBillAccId+","+uuidBillInstId+",0,"+amount+",0,0,0)";
            } else {
                return reject('Invalid transactionType  in the request payload');
            }
        } else {
            return reject('transactionType Not defined in the request payload');
        }
        logger.info('Inserting record to the transaction_summary table - > query - > ',queryIs);
        return cassaClient.execute(queryIs, [], function(err, result){
            if(!err){
                logger.info('Record is inserted successfully to the transaction_summary table');
                resolve({'response':{'code':'0','desc':'Record is inserted successfully to the transaction_summary table'}});
            }else{
                logger.error(err);
                reject(err);
            }
        });
    });
}


function updateDataIntoTransactionSummary(requestPayload,uuidBillInstId,uuidBillAccId,dbPayload) {
    return new Promise(function(resolve,reject){
        logger.info('Request Payload Is::',requestPayload);
        logger.info('DB Payload Is::',dbPayload);
        let transactionFlag = requestPayload.transaction;
        let debitAmount = 0;
        let creditAmount = 0;
        let paymentAmount = 0;
        let billingCycleInstId = uuidBillInstId;
        let billingAccountId = uuidBillAccId;

        if(transactionFlag.toString().toUpperCase() === 'PAYMENT'){
            paymentAmount = parseFloat(requestPayload.amount) + parseFloat(dbPayload.payment);
            debitAmount =   parseFloat(dbPayload.debit);
            creditAmount =  parseFloat(dbPayload.credit);
        } else if(transactionFlag.toString().toUpperCase() === 'DEBIT'){
            debitAmount = parseFloat(requestPayload.amount) + parseFloat(dbPayload.debit);
            paymentAmount = parseFloat(dbPayload.payment);
            creditAmount =  parseFloat(dbPayload.credit);
        } else if(transactionFlag.toString().toUpperCase() === 'CREDIT'){
            creditAmount = parseFloat(requestPayload.amount) + parseFloat(dbPayload.credit);
            paymentAmount = parseFloat(dbPayload.payment);
            debitAmount =  parseFloat(dbPayload.debit);
        } else {
            logger.error ('Invalid transaction Flag');
            reject('Invalid transaction Flag');
        }

        const query = "UPDATE transaction_summary set credit= "+ creditAmount +", debit = "+debitAmount+", payment = "+ paymentAmount +" WHERE id = "+billingAccountId+" AND billing_cycle_instance_id= "+billingCycleInstId;
        logger.info('Updating credit, debit and payment into transaction_summary table -> query - > ',query);
        cassaClient.execute(query, [], function (err, result) {
            if (!err) {
                logger.info("Updated credit, debit and payment into transaction_summary successfully");
                resolve(result);
            } else {
                logger.error("Failed to update credit, debit and payment in transaction_summary");
                reject('Unable to Update in Transaction Summary');
            }
        });
    });
}

function resetTransactionSummaryAmount(requestPayload){
    return new Promise(function (resolve, reject) {
        let billingAccountId = requestPayload.identifier;
        logger.info('Request came to resetTransactionSummaryAmount function -> request payload - > ',billingAccountId);
        const query = "select * from transaction_summary where id = "+billingAccountId;
        logger.info('Getting details from transaction_summary table for reset amount- > query - > ',query);
        cassaClient.execute(query, [], function (err, result) {
            if (!err) {
                let resultData = result.rows;
                let resultLength = result.rows.length;
                let arr_billing_cycle_instance_id = [];
                if(resultLength > 0){
                    resultData.forEach(data => {
                        logger.info("Got data from transaction_summary and billing_cycle_instance_id is - ",data.billing_cycle_instance_id);
                        arr_billing_cycle_instance_id.push(data.billing_cycle_instance_id);
                    });
                    logger.info("Got all billing_cycle_instance_id from transaction_summary to reset amount",arr_billing_cycle_instance_id);
                    resetTransactionSummaryData(billingAccountId,arr_billing_cycle_instance_id);
                } else {
                    logger.error("No billing_cycle_instance_id found in the transaction_summary table - > query - > ",query);
                    reject("No billing_cycle_instance_id found in the transaction_summary table");
                }
            } else {
                logger.error("Failed to date from transaction_summary table to reset amount -> query - >",query);
                reject("Failed to resent amount for transaction_summary");
            }
        });
    });
}

function resetTransactionSummaryData(billingAccountId,arr_billing_cycle_instance_id){
    return new Promise(function (resolve, reject) {
        const query = "UPDATE transaction_summary set credit= 0 , debit = 0, payment = 0 WHERE id = "+billingAccountId+" AND billing_cycle_instance_id IN ("+arr_billing_cycle_instance_id+")";
        logger.info('Resetting credit, debit and payment for transaction_summary - > query - > ',query);
        cassaClient.execute(query, [], function (err, result) {
            if (!err) {
                logger.info("Reset amount in transaction_summary done successfully");
                resolve(result);
            } else {
                logger.error("Failed to reset credit, debit and payment for transaction_summary");
                reject("Failed to reset transaction_summary");
            }
        });
    });
}


module.exports = {
    getInvoiceById: getInvoiceById,
    insertInvoice: insertInvoice,
    getInvoiceByBillingAccountIdAndBillRunId: getInvoiceByBillingAccountIdAndBillRunId,
    getInvoiceIdsByBillingRunId: getInvoiceIdsByBillingRunId,
    updateInvoiceStatus : updateInvoiceStatus,
    getInvoiceNumberFromSampleBillRun : getInvoiceNumberFromSampleBillRun,
    checkPaymentsCreditDebitDetails : checkPaymentsCreditDebitDetails,
    insertPaymentsCreditsDebits : insertPaymentsCreditsDebits,
    deleteRecordPaymentsCreditsDebits : deleteRecordPaymentsCreditsDebits,
    insertIntoTransactionSummary : insertIntoTransactionSummary,
    resetTransactionSummaryAmount : resetTransactionSummaryAmount,
    executeQuery : executeQuery
};
