/* jshint strict:true */
/* jshint node: true */
'use strict';
const NR = require("node-resque");
const config = require('config');
const connectionDetails = config.get('redis');
const jobs = require('./jobs');

var multiWorker = new NR.multiWorker({
  connection: connectionDetails,
  queues: ['decorate', 'notify'],
  minTaskProcessors:   1,
  maxTaskProcessors:   100,
  checkTimeout:        1000,
  maxEventLoopDelay:   10,
  toDisconnectProcessors: true,
}, jobs);

multiWorker.start();
