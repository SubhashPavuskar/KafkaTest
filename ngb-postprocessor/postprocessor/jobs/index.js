/* jshint strict:true */
/* jshint node: true */

'use strict';
const decoratorJob = require('./decorator_job');
const notificationJob = require('./notification_job');
module.exports = {
  "decorate": {
    plugins: [ 'jobLock', 'retry', "queueLock"],
    pluginOptions: {
      jobLock: {},
      queueLock : {},
      retry: {
        retryLimit: 3,
        retryDelay: (1000 * 5),
      }
    },
    perform: function(invoices, newTopicName, totalNumberOfInvoices, billRunType, notificationTopic, invoiceSampleBatchID, callback) {
      decoratorJob.decorate(invoices, newTopicName, totalNumberOfInvoices, billRunType, notificationTopic, invoiceSampleBatchID)
       .then(() => callback())
       .catch((err) => callback(err));
    }
  },
  "notify": {
    plugins: [ 'jobLock', 'retry', "queueLock"],
    pluginOptions: {
      jobLock: {},
      queueLock : {},
      retry: {
        retryLimit: 3,
        retryDelay: (1000 * 5),
      }
    },
    perform: function(billRunId, notificationTopic, payload, callback) {
      notificationJob.notify(billRunId, notificationTopic, payload)
       .then(() => callback())
       .catch((err) => callback(err));
    }
  }
};
