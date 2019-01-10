//const _ = require('lodash');

module.exports={
    _exNU:function (num){
        let _chkAmt=num;
        _chkAmt === null ? _chkAmt=0:_chkAmt;
        _chkAmt === "Null"? _chkAmt=0:_chkAmt;
        _chkAmt === "NULL"? _chkAmt=0:_chkAmt;
        _chkAmt === "undefined" ? _chkAmt=0:_chkAmt;
        _chkAmt === "Undefined" ? _chkAmt=0:_chkAmt;
        _chkAmt === isNaN(_chkAmt) ? _chkAmt=0:_chkAmt;
        return _chkAmt;
    },

    _strValidation:function(str){
        let _chkStr=str;
        // if(_.startCase(_.toLower(_chkStr)) === _chkStr){
        return _chkStr.charAt(0) === _chkStr.charAt(0).toUpperCase() && _chkStr.substring(1,_chkStr.length) === _chkStr.substring(1,_chkStr.length).toLowerCase();
    },
    callNotification :function(requestPayload, failureReason){
        requestPayload.reason = failureReason;
        let notificationPayload = JSON.stringify(requestPayload);
        producer.send([{topic: rejectionNotification, messages: [notificationPayload]}], (err, data) => {
            if(data){
                logger.info("Notification Success", data);
            }
            if(err){
                logger.info("Notification FAILED", err);
            }
        });
    }
};