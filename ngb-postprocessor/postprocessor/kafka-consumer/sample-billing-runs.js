/* jshint strict:true */
/* jshint node: true */
'use strict';
const config = require('config');
const connectionString = `${config.get('kafka.host')}:${config.get('kafka.port')}`;
const consumerConfig = config.get('kafka.sampleBillConsumerConfig');
consumerConfig["metadata.broker.list"] = connectionString;
const Kafka = require('node-rdkafka');
const consumer = new Kafka.KafkaConsumer(consumerConfig);
const topicName = config.get('kafka.sampleBillRunIdTopic');

const logger = require('../helpers/logger');

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
    logger.info('Got request to get sample bills -> ',message);
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


