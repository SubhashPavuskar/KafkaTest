'use strict';
var fs = require('fs');
const config = require('config');
const connectionString = `${config.get('kafka.host')}:${config.get('kafka.port')}`;
const consumerConfig = config.get('kafka.kafkaProducerConfig');
consumerConfig["metadata.broker.list"] = connectionString;
consumerConfig["dr_cb"] = true;
const Kafka = require('node-rdkafka');


const logger = require('../helpers/logger');

exports.sendMessage = function (topicName, message, counter) {
    topicName = "z2";
    return new Promise((resolve, reject) => {
        var contents = fs.readFileSync('message6.json', 'utf8');
        console.log("length - - ",contents.length);
        const producer = new Kafka.Producer(consumerConfig);
        logger.info("Came to Produces to send message for topic  - ", topicName);

        //logging debug messages, if debug is enabled
        try {
            producer.on('event.log', function (log) {
                logger.info(log);
            });

            //logging all errors
            producer.on('event.error', function (err) {
                logger.error('Error from producer');
                logger.error(err);
            });

            producer.on('delivery-report', function (err, report) {
                logger.info("error while sending - ", err);
                logger.info('delivery-report: ' + JSON.stringify(report));
                if(err){
                    logger.error("Failed to push message to kafka - Error -> ",err);
                    resolve({});
                } else {
                    logger.info("Message is pushed to kafka - Report -> ",report);
                    resolve(report);
                }
                producer.disconnect();
            });

            producer.on('ready', function (arg) {
                logger.info('producer ready.' + JSON.stringify(arg));
                let partition = -1;
                //var value = Buffer.from(message);
                var value = Buffer.from(contents);

                producer.produce(topicName, partition, value);
                producer.setPollInterval(100);

            });

            producer.on('disconnected', function (arg) {
                console.log('producer disconnected. ' + JSON.stringify(arg));
            });

            //starting the producer
            producer.connect();

        } catch (err) {
            logger.error('A problem occurred when sending our message');
            logger.error(err);
        }
    });
}


