/* jshint strict:true */
/* jshint node: true */

'use strict';

const _ = require('lodash');
const mysql = require('../mysql');
const promise = require("bluebird");
const YAML  = require('yamljs');
const config = require('config');
const logger = require('./logger');
const cassaClient = require('../cassandra');

const tablesConfig = YAML.load('./config/tables.yml');
const translationsConfig = YAML.load('./config/translations.yml');

function getDecoratedInvoice(rawInvoice) {
    return addMissingData(rawInvoice).then(addTransactionSummery).then(addTranslations);
}

function addTransactionSummery(rawInvoice) {
    logger.info("Request came to add transaction summery function");
    return new Promise(function (resolve, reject) {
        let billingCycleInstanceId = _.get(rawInvoice, 'metadata.billingCycleInstanceId') || '';
        let billingAccountId = _.get(rawInvoice, 'metadata.billingAccountId') || '';
        let transactionSummary = {
            previousBalance: 0,
            payments: 0,
            totalDue: 0,
            toBePaid : parseFloat(_.get(rawInvoice, 'invoiceSummary.toBePaid')) || 0,
            adjustments : 0
        };
        if (billingCycleInstanceId && billingAccountId) {
            let query = "select * from transaction_summary where id = " + billingAccountId + " and billing_cycle_instance_id = " + billingCycleInstanceId ;
            logger.info("Querying to get transaction summery - > ", query);
            cassaClient.executeQuery(query).then(function (data) {
                if (data.rowLength > 0) {
                    let currentTransactionSummary = {
                        credits: parseFloat(_.get(data, 'rows[0].credit')) || 0,
                        debits: parseFloat(_.get(data, 'rows[0].debit')) || 0,
                        payments: parseFloat(_.get(data, 'rows[0].payment')) || 0,
                        toBePaid : parseFloat(_.get(rawInvoice, 'invoiceSummary.toBePaid')) || 0,
                        totalDue: 0,
                        adjustments: 0,
                        previousBalance: 0
                    };
                    currentTransactionSummary.adjustments = currentTransactionSummary.credits - currentTransactionSummary.debits;
                    logger.info("Calculated adjustments - ", currentTransactionSummary);
                    let previousBillingCycleInstanceId = _.get(rawInvoice, 'metadata.previousBillingCycleInstanceId') || '';
                    if (previousBillingCycleInstanceId && billingAccountId) {
                        let getLastTransactionSummery = "select * from transaction_summary where id = " + billingAccountId + " and billing_cycle_instance_id = " + previousBillingCycleInstanceId ;
                        logger.info("Querying to get previous transaction summery details - ",getLastTransactionSummery);
                        cassaClient.executeQuery(getLastTransactionSummery).then(function (data) {
                            if (data.rowLength > 0) {
                                let previousTransactionSummary = {
                                    credits: parseFloat(_.get(data, 'rows[0].credit')) || 0,
                                    debits: parseFloat(_.get(data, 'rows[0].debit')) || 0,
                                    payments: parseFloat(_.get(data, 'rows[0].payment')) || 0,
                                    invoices: parseFloat(_.get(data, 'rows[0].invoice')) || 0,
                                    due_amount : parseFloat(_.get(data, 'rows[0].due_amt')) || 0
                                };
                                currentTransactionSummary.previousBalance = previousTransactionSummary.due_amount;
                                logger.info("Calculating previous balance - ", currentTransactionSummary);
                                let addedPaymentDetails = addPaymentDetails(rawInvoice, currentTransactionSummary, true);
                                resolve(addedPaymentDetails);
                            } else {
                                logger.info("No data found for previous transaction summery");
                                let addedPaymentDetails = addPaymentDetails(rawInvoice, currentTransactionSummary, true);
                                resolve(addedPaymentDetails);
                            }
                        }, function () {
                            logger.info("Got error for previous transaction summery ");
                            let addedPaymentDetails = addPaymentDetails(rawInvoice, transactionSummary, true);
                            resolve(addedPaymentDetails);
                        });
                    } else {
                        logger.info("previousBillingCycleInstanceId or billingAccountId not there in json");
                        let addedPaymentDetails = addPaymentDetails(rawInvoice, currentTransactionSummary, true);
                        resolve(addedPaymentDetails);
                    }
                } else {
                    logger.info("No data found for current transaction summery");
                    let addedPaymentDetails = addPaymentDetails(rawInvoice, transactionSummary, false);
                    resolve(addedPaymentDetails);
                }
            }, function () {
                logger.info("Got error for current transaction summery ");
                let addedPaymentDetails = addPaymentDetails(rawInvoice, transactionSummary, false);
                resolve(addedPaymentDetails);
            });
        } else {
            logger.info("billingCycleInstanceId or billingAccountId not there in json");
            let addedPaymentDetails = addPaymentDetails(rawInvoice, transactionSummary, false);
            resolve(addedPaymentDetails);
        }
    });
}
function addPaymentDetails(rawInvoice, paymentDetails, updateDueToTable) {
    if (_.has(rawInvoice, 'invoiceSummary')) {
        rawInvoice.invoiceSummary.payments = paymentDetails.payments;
        rawInvoice.invoiceSummary.adjustments = paymentDetails.adjustments;
        rawInvoice.invoiceSummary.previousBalance = paymentDetails.previousBalance;
        rawInvoice.invoiceSummary.totalDue = paymentDetails.previousBalance + paymentDetails.toBePaid + paymentDetails.adjustments - paymentDetails.payments;
        let billingCycleInstanceId = _.get(rawInvoice, 'metadata.billingCycleInstanceId') || '';
        let billingAccountId = _.get(rawInvoice, 'metadata.billingAccountId') || '';
        let billType = rawInvoice.metadata.billingRunType;
        logger.info("Added payment details to json  - - ",paymentDetails , "  update to the table - ",updateDueToTable && _.includes(config.updateDueAmtForBillType, billType));
        if(billingCycleInstanceId && billingAccountId && updateDueToTable && _.includes(config.updateDueAmtForBillType, billType)){
            let updateDueQuery = "update transaction_summary set due_amt="+rawInvoice.invoiceSummary.totalDue+" where id = " + billingAccountId + " and billing_cycle_instance_id = " + billingCycleInstanceId ;
            return cassaClient.executeQuery(updateDueQuery).then(function (data) {
                logger.info("Successfully updated due amount to the transaction_summary table.");
                return rawInvoice;
            }, function () {
                logger.info("Failed to updated due amount to the transaction_summary table.");
                return rawInvoice;
            });
        } else {
            return rawInvoice;
        }
    } else {
        logger.info("invoiceSummary object not found in json");
        return rawInvoice;
    }
}


function addMissingData(rawInvoice) {
  logger.info('Request came to add missing Data for decorator');
  let requests = _.compact(tablesConfig.primaryTables.map((table) => {
    let primaryKeyValue =  _.get(rawInvoice, table.rawInvoiceKey);
    if (!primaryKeyValue) return;

    let tableName = table.name;
    let columns = Object.keys(table.columns);
    let primaryKeyColumnName  = table.primaryKeyColumnName;

    let idQueryPart = table.binaryPrimaryKey ? `UNHEX(REPLACE('${primaryKeyValue}', '-', ''))` : `'${primaryKeyValue}'`;
    var dynamicQuery;
    if(tableName.toString().toUpperCase() !== 'SERVICE_ACCOUNTS'){
      dynamicQuery = `SELECT ${columns.join(', ')} FROM ${tableName} WHERE ${primaryKeyColumnName} = ${idQueryPart}`;
    }
    else if(tableName.toString().toUpperCase() === 'SERVICE_ACCOUNTS'){
      dynamicQuery = `SELECT ${columns.join(', ')}, HEX(ID) AS HEXID FROM ${tableName} WHERE ${primaryKeyColumnName} = ${idQueryPart}`;
    }
    logger.info(":::::::::::::::::::-->dynamicQuery::",dynamicQuery);

    if(tableName.toString().toUpperCase() !== 'SERVICE_ACCOUNTS'){
      return mysql.execute(dynamicQuery).then((result) => {
        let hash = {};
  
        if (result) {
          columns.forEach((column) => _.set(hash, table.columns[column], result[column] !== null ? result[column] : ''));
        }
  
        return hash;
      });
    } 
    else if(tableName.toString().toUpperCase() === 'SERVICE_ACCOUNTS'){
      return mysql.executeQueryForMultipleRecords(dynamicQuery).then((result) => {
          let hash = {
            "tempServiceAccounts":[]
          };
    
          if (result) {
            result.forEach(function(column, index) {
              let hashObjToPush = {
                "metadata": {
                  "serviceAccountNumber": "",
                  "serviceEmailId": "",
                  "serviceAccountId": "",
                  "serviceName":"",
                  "itemizedFlag": ""
                }
              };
              let indDbJsonObj = result[index];
              hashObjToPush.metadata.serviceAccountNumber = indDbJsonObj.SERVICE_ACCOUNT_NO;
              hashObjToPush.metadata.serviceEmailId = indDbJsonObj.EMAIL_ID;
              hashObjToPush.metadata.serviceAccountId = indDbJsonObj.HEXID;
              hashObjToPush.metadata.serviceName = indDbJsonObj.NAME;
              hashObjToPush.metadata.itemizedFlag = indDbJsonObj.ITEMIZED_FLAG;

              hash.tempServiceAccounts.push(hashObjToPush);
            });
          }
          return hash;
        });
    }
  }));

  return promise.all(requests).then((additionalFields) => {
    const combinedFields = _.merge({}, ...additionalFields);
    const staticFields = tablesConfig.statics;
    const invoice = _.merge({}, rawInvoice, combinedFields, staticFields);
    logger.info(" Added additional fields  ");
    let tempServiceAccountsArr = invoice.tempServiceAccounts;
    if(_.isArray(tempServiceAccountsArr) && _.isArray(invoice.serviceAccounts)) {
      if(tempServiceAccountsArr.length > 0){
        invoice.serviceAccounts.forEach(function(objInvoiceArr){
          let inVoiceMetadata = objInvoiceArr.metadata;
          let inVoiveServiceAccountId = inVoiceMetadata.serviceAccountId;
          tempServiceAccountsArr.forEach(function(objTempServiceAccountsArr){
            let tempMetadata = objTempServiceAccountsArr.metadata;
            let tempServiceAccountId = tempMetadata.serviceAccountId;

            inVoiveServiceAccountId = inVoiveServiceAccountId.replace(/-/g, '');
            inVoiveServiceAccountId = inVoiveServiceAccountId.toString().toUpperCase();

            tempServiceAccountId = tempServiceAccountId.replace(/-/g, '');
            tempServiceAccountId = tempServiceAccountId.toString().toUpperCase();

            logger.info('inVoiveServiceAccountId::',inVoiveServiceAccountId);
            logger.info('tempServiceAccountId::',tempServiceAccountId);
            if (inVoiveServiceAccountId === tempServiceAccountId) {             
              objInvoiceArr.serviceAccountNumber = objTempServiceAccountsArr.metadata.serviceAccountNumber;
              objInvoiceArr.metadata.serviceEmailId = objTempServiceAccountsArr.metadata.serviceEmailId;
              objInvoiceArr.metadata.serviceName = objTempServiceAccountsArr.metadata.serviceName;
              if(objTempServiceAccountsArr.metadata.itemizedFlag === 'Y'){
                objInvoiceArr.itemizedFlag ='Y';
              } else {
                objInvoiceArr.itemizedFlag ='N';
              }
            }
          });  
        });
      }
    }
    if(invoice.tempServiceAccounts){
      delete invoice.tempServiceAccounts;
    }
    return invoice;
  });
}

function addTranslations(rawInvoice) {
  let billLanguage =  _.get(rawInvoice, translationsConfig.billLanguageKey);
  let translationKeysValuesAndPaths = _.flatMap(translationsConfig.translations, (translationKey) => getKeysValuesAndPathsByKey(rawInvoice, translationKey));
  logger.info("Request came to add translations with row invoice ");
  return mysql.getMultipleTranslations(billLanguage, translationKeysValuesAndPaths)
  .then((translations) => {
    logger.info("Translation for invoice - ", translations);
      if(translations) {
          translations.forEach((obj) => {
              if (obj.translation) {
                  _.set(rawInvoice, obj.path, obj.translation);
              }
          });
      }
    return JSON.stringify(rawInvoice);
  })
  .catch((err) => logger.error(`ERROR: ${err}`));
}

function getKeysValuesAndPathsByKey(obj, key, path='') {
  let objects = [];
  Object.keys(obj).forEach((k) => {
    if ( obj[k] && typeof obj[k] == 'object') {
      objects = objects.concat(getKeysValuesAndPathsByKey(obj[k], key, _.isEmpty(path) ? k : [path, k].join('.')));
    } else if (k == key) {
      objects.push({path: [path, key].join('.'), jsonKey: key, jsonValue: obj[k]});
    }
  });
  return objects;
}

module.exports.getDecoratedInvoice = getDecoratedInvoice;
