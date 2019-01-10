const config = require('config');
const promise = require("bluebird");
const kafka = promise.promisifyAll(require('kafka-node'));
const cassaClient = require('../cassandra');
const logger = require('./logger');
const connectionString = `${config.get('kafka.host')}:${config.get('kafka.port')}`;
const YAML  = require('yamljs');
const mysql = require('../mysql');
const paymentsCreditsDebitsTopic = config.get('kafka.paymentsCreditsDebitsTopic');
const rejectionNotification = config.get('kafka.rejectionNotification');
const data_constants = require("../config/constants");
let isJSON = require('is-valid-json');
let Consumer = kafka.Consumer;
let client = new kafka.Client(connectionString + '/');
const producer = new kafka.HighLevelProducer(client);
const _util=require('./utility');
const hexToUuid = require('hex-to-uuid');

let paymentsCreditsDebitsConsumer = new Consumer(
    client,
    [],
    {fromOffset: true}
);

paymentsCreditsDebitsConsumer.addTopics([
    {topic: paymentsCreditsDebitsTopic, partition: 0, offset: 0}
], () => logger.info('topic ' + paymentsCreditsDebitsTopic + ' added to consumer for listening'));

var callNotification = function(requestPayload, failureReason){
    requestPayload.reason = failureReason;
    let notificationPayload = JSON.stringify(requestPayload);
    logger.info("Rejection notification from payment_debit_credit -> message - > ", notificationPayload);
    producer.send([{topic: rejectionNotification, messages: [notificationPayload]}], (err, data) => {
        if(data){
            logger.info("Notification Success", data);
        }
        if(err){
            logger.info("Notification FAILED", err);
        }
    });
};

function actionDCC(payload){
    //check IdentifierType and query the Table
    let  dynamicQuery;
    let identifierTypeFlag = payload.identifierType;
    let identifierVal = payload.identifier;
    let actionType = payload.action;
    let tableName;
    let billingAccountID;
	
    if(identifierTypeFlag && identifierVal){
		
		identifierVal = identifierVal.replace(/-/g,'');

        if(identifierTypeFlag.toString().toUpperCase() === 'ACCOUNT'){
            dynamicQuery = "SELECT HEX(ID) AS ID,  HEX(BILLING_CYCLE_ID) AS BILLING_CYCLE_ID FROM BILLING_ACCOUNTS WHERE  ID=UNHEX('"+identifierVal+"')";
            tableName = "BILLING_ACCOUNTS";
        } else if(identifierTypeFlag.toString().toUpperCase() === 'SERVICE'){
            dynamicQuery = "SELECT HEX(ID) FROM SERVICE_ACCOUNTS WHERE  ID=UNHEX('"+identifierVal+"')";
            tableName = "SERVICE_ACCOUNTS";
        } else {
            //Need Send notification
            logger.info('Invalid IdentifierType Flag :: ',identifierTypeFlag);
            let reason = 'Invalid IdentifierType Flag';
            callNotification(payload, reason);
        }
        logger.info('Query to get billing account id or service account id - > query - > ',dynamicQuery);
        mysql.execute(dynamicQuery).then((result) => {
            if(result){
                logger.info('Data found for identifier :: '+identifierVal+' :: result -> '+JSON.stringify(result));
                billingAccountID = result;

                //It will not be duplicated, it will be overwritten unless you place the data in a different column family or keyspace, then you can duplicate it
                cassaClient.insertPaymentsCreditsDebits(payload).then(function(resp){
                }, function(err){
                    //Need Send notification
                    logger.error("error - ",err);
                    let reason = 'Error Querying Mysql';
                    callNotification(payload, reason);
                });


                //NGB-2636 Inserting into New Table Only for Action=New And Identifier

                if(identifierVal && actionType){
                    if((identifierTypeFlag.toString().toUpperCase() === 'ACCOUNT') && (actionType.toString().toUpperCase() === 'NEW')){
                        let billingCycleID = billingAccountID.BILLING_CYCLE_ID;
                        logger.info('BILLING_CYCLE_ID Is::',billingAccountID.BILLING_CYCLE_ID);
                        let dateIs = payload.date;
                        if(dateIs){
                            let getBillingCycleInstanceIdQuery = "SELECT HEX(ID)  AS  BILLING_CYCLE_INSTANCES_ID FROM  BILLING_CYCLE_INSTANCES WHERE  DATE('"+dateIs+"') BETWEEN  START_DATE  AND END_DATE AND (HEX(BILLING_CYCLE_ID) = '"+billingCycleID+"')";

                            logger.info('Query Formed:: ',getBillingCycleInstanceIdQuery);
                            mysql.execute(getBillingCycleInstanceIdQuery).then((result) => {
                                logger.info('----------------------------->result',result);
                                if(result)
                                {
                                    logger.info('=================>IS',result);
                                    let billingCycleInstnceId = result;
                                    cassaClient.insertIntoTransactionSummary(payload,billingCycleInstnceId,billingAccountID).then(function (responseData) {


                                    }, function(err){
                                        //Need Send notification
                                        logger.info("Error processing the request to cassandra ",err);
                                        let reason = 'Error processing the request to cassandra';
                                        callNotification(payload, reason);
                                    });

                                } else {
                                    let reason = 'Unable to find the billing Cycle Instance ID based on the parameter passed to  BILLING_CYCLE_INSTANCES table';
                                    logger.info("error - ",reason);
                                    callNotification(payload, reason);
                                }
                            });
                        }
                    } else {
                        let reason = 'Unable to find the date in the request payload';
                        logger.info("error - ",reason);
                        callNotification(payload, reason);
                    }
                }

            }
            else{
                //Need Send notification
                logger.info('No data found for identifier:: '+identifierVal+' in the table:: '+tableName);
                let reason = 'No data found for identifier:: '+identifierVal+' in the table:: '+tableName;
                callNotification(payload, reason);
            }
        });
    } else{
        //Need Send notification
        logger.info('IdentifierType Flag is:: ',IdentifierTypeFlag);
        let reason = 'Invalid Identifier';
        callNotification(payload, reason);
    }
}

paymentsCreditsDebitsConsumer.on('message', function(message) {
    logger.info('Got request to  paymentsCreditsDebits-> ',message);
    if( isJSON(message.value) ) {
        let payload = JSON.parse(message.value);
        let _payloadID = payload.id;
        let _trans = payload.transaction.toString().toUpperCase();
        let _validTrs = _trans === "DEBIT" || _trans === "CREDIT" || _trans === "PAYMENT";
        let actionFlag = payload.action;
        const _ckNewExist = actionFlag.toString().toUpperCase() === data_constants.new || actionFlag.toString().toUpperCase() === data_constants.existing;
        const validatereq=_util._strValidation(actionFlag) && _util._strValidation(payload.transaction) && _util._strValidation(payload.identifierType);

        if(!validatereq){
            logger.error('Invalid request parameters.Error in one of these value action Flag-'+actionFlag+', Transaction-'+payload.transaction+',identifierType-'+payload.identifierType);
            let reason = 'Invalid request parameters.Error in one of these value action Flag-'+actionFlag+', Transaction-'+payload.transaction+',identifierType-'+payload.identifierType;
            callNotification(payload, reason);
            return;
        }

        if(actionFlag && _validTrs ){
            if(_ckNewExist && _payloadID !== "" ){
                logger.info('Inside Flag --> ',payload.identifierType);
                cassaClient.checkPaymentsCreditDebitDetails(payload).then(function (responseData) {
                    let CRlength = responseData.rowLength;
                    logger.info("Number of rows present in cassandra with the ID requested-------"+CRlength);
                    if(CRlength === 0 && actionFlag.toString().toUpperCase() === data_constants.new){
                        logger.info("-------Inserting fresh Record----------");
                        actionDCC(payload);
                    }else if(CRlength >= 1 && actionFlag.toString().toUpperCase() === data_constants.existing){
                        logger.info("-------Inserting data with on existing ID with Existing request----------");
                        actionDCC(payload);
                    }else{
                        //Need Send notification
                        logger.error('Invalid payload action and id,identifier..');
                        let reason = 'Invalid payload action and id,identifier..';
                        callNotification(payload, reason);
                    }
                }, function(err){
                    //Need Send notification
                    logger.error("Error processing the request to cassandra ",err);
                    let reason = 'Error processing the request to cassandra';
                    callNotification(payload, reason);
                });

            }  else if(actionFlag.toString().toUpperCase() === data_constants.delete){
                //Need Handle Existing data
                if(_payloadID === null || _payloadID.toUpperCase() === "NULL" || _payloadID ==="" ) {
                    cassaClient.resetTransactionSummaryAmount(payload).then(function (resp) {
                        logger.info("Response From Cansandra========>DELETE", resp);
                    }, function (err) {
                        //Need Send notification
                        logger.error("error - ", err);
                        let reason = 'Error processing the request to cassandra';
                        callNotification(payload, reason);
                    });
                    cassaClient.deleteRecordPaymentsCreditsDebits(payload).then(function (resp) {
                        logger.info("Response From Cansandra========>DELETE", resp);
                    }, function (err) {
                        //Need Send notification
                        logger.error("error - ", err);
                        let reason = 'Error processing the request to cassandra';
                        callNotification(payload, reason);
                    });
                }else{
                    _initiateDeleteforID(payload);
                }
            } else {
                //Need Send notification
                logger.error("INVALID ACTION FLAG");
                let reason = 'Invalid Action Flag';
                callNotification(payload, reason);
            }

        } else{
            //Need to send notitication
            logger.info("Transaction should be either Payment/Credit/Debit");
            logger.info('Flag Is ::-> ',payload.action);
            let reason = 'Action Flag is Undefined';
            callNotification(payload, reason);
        }
    }else{
        logger.error('Kafka Request Error. please check correct JSON data on Request -> ', message.value);
        let reason = 'Kafka Request Error. Request payload is Invalid';
        callNotification(message.value, reason);
    }
});

function _initiateDeleteforID(payload){
    let identifierTypeFlag = payload.identifierType;
    let identifierVal = payload.identifier.replace(/-/g,'');
    let _BillingCycleID='';
    let dynamicQuery="";
    //logger.error("-----------identifier---------",hexToUuid(identifierVal.toLowerCase()));
    if(identifierTypeFlag.toString().toUpperCase() === 'ACCOUNT'){
        dynamicQuery = "SELECT HEX(BILLING_CYCLE_ID) AS BILLING_CYCLE_ID FROM BILLING_ACCOUNTS WHERE  ID=UNHEX('"+identifierVal+"')";
    } else if(identifierTypeFlag.toString().toUpperCase() === 'SERVICE'){
        dynamicQuery = "SELECT HEX(BILLING_CYCLE_ID) AS BILLING_CYCLE_ID FROM SERVICE_ACCOUNTS WHERE  ID=UNHEX('"+identifierVal+"')";
    } else {
        //Need Send notification
        logger.error('Invalid identifier flag::-> ',identifierTypeFlag);
        let reason = 'Invalid identifier flag';
        callNotification(payload, reason);
    }
    mysql.execute(dynamicQuery).then((result) => {
        if (result) {
            _BillingCycleID = result.BILLING_CYCLE_ID;
            let _date ='';
            let _amount = '';
            let _findExist='';
            logger.info("action type is ",data_constants.action_new);
            if( payload.id === null || payload.id.toUpperCase() === "NULL" || payload.id ===""){
                _findExist = "select date,amount from payments_credits_debits where identifier = '" + payload.identifier + "' and action='"+data_constants.action_new+"' ALLOW FILTERING";
            }else{
                _findExist = "select date,amount from payments_credits_debits where identifier = '" + payload.identifier + "' and id='" + payload.id + "' and action='"+data_constants.action_new+"' ALLOW FILTERING";
            }

            cassaClient.executeQuery(_findExist).then(function (result) {
                _date=result.rows[0].date;
                _amount=result.rows[0].amount;
                logger.info("result from payments_credits_debits--", _date,_amount);
                _updateTras(_BillingCycleID,_date,payload.identifier,_amount,payload);
            }).catch((err) =>
                {
                    logger.error('Could not find record in payment debit credit table for given Identifer and ID::-> ',err);
                    let reason = 'Could not find record in payment debit credit table for given Identifer and ID..';
                    return callNotification(payload, reason);
                }
            );
        }else{
            logger.error("no data found",result);
        }
    }).catch((err) =>
        {
            logger.error('Could not find record in payment debit credit table::-> ',err);
            let reason = 'Could not find record in payment debit credit table..';
            return callNotification(payload, reason);
        }
    );
}
function _updateTras(_BillingCycleID,_dates,_IDFR,_AMT,payload){
    let _BINSID='';
    let _BID='';
    let _srcAmount=0;
    let _transaction = payload.transaction;

    let getBillingCycleInstanceIdQuery = "SELECT HEX(ID)  AS  BILLING_CYCLE_INSTANCES_ID FROM  BILLING_CYCLE_INSTANCES WHERE  DATE('"+_dates+"') BETWEEN  START_DATE  AND END_DATE AND (HEX(BILLING_CYCLE_ID) = '"+_BillingCycleID+"')";
    mysql.execute(getBillingCycleInstanceIdQuery).then((result) => {
        if(result){
            _BINSID=hexToUuid(result.BILLING_CYCLE_INSTANCES_ID.toLowerCase());
            _BID=_IDFR.toLowerCase();
            let _dynamcolumn=data_constants[_transaction];
            logger.info("________",_dynamcolumn);
            logger.info("ID from BILLING_CYCLE_INSTANCES_ID0-------00000000------",_BINSID,_BID);
            let _getTransSum="select "+_dynamcolumn+" from transaction_summary where billing_cycle_instance_id="+_BINSID+" and id="+_BID+" ALLOW FILTERING";
            cassaClient.executeQuery(_getTransSum).then(function (result) {
                if(_dynamcolumn == "payment"){
                    _srcAmount=parseFloat(_util._exNU(result.rows[0].payment));
                }else if(_dynamcolumn == "credit"){
                    _srcAmount=parseFloat(_util._exNU(result.rows[0].credit));
                }else if(_dynamcolumn == "debit"){
                    _srcAmount=parseFloat(_util._exNU(result.rows[0].debit));
                }else{
                    logger.info("Invalid transaction flag");
                }


                // _srcAmount=number(result.rows[0].payment);
            }).then(function(){
                let _setTransSum="UPDATE transaction_summary set "+_dynamcolumn+"="+(parseFloat(_srcAmount-_util._exNU(_AMT)))+" where billing_cycle_instance_id="+_BINSID+" and id="+_BID;
                logger.info("update query is ",_setTransSum);
                cassaClient.executeQuery(_setTransSum).then(function () {
                    logger.info("Amount has been updated in transaction_summary");
                }).then(function(){
                    cassaClient.deleteRecordPaymentsCreditsDebits(payload).then(function(){
                        logger.info("___________Deleted record_____________");
                    }).catch((err) => {
                        logger.error('Could not Delete from payments_credit_debit.',err);
                        let reason = 'Could not Delete from payments_credit_debit.';
                        return callNotification(payload, reason);
                    });
                }).catch((err) =>
                {
                    logger.error('Failed to update in Transaction Summary table:-> ',err);
                    let reason = 'Failed to update in Transaction Summary table..';
                    return callNotification(payload, reason);
                });
            }).catch((err) =>
            {
                logger.error('Failed to fetch the record in Transaction Summary table:-> ',err);
                let reason = 'Failed to fetch the record in Transaction Summary table..';
                return callNotification(payload, reason);
            });
        }else{
            logger.error('Could not find record in billing cycle instances');
            let reason = 'Could not find record in billing cycle instances..';
            return callNotification(payload, reason);
        }
    });
}