/* jshint strict:true */
/* jshint node: true */

'use strict';

const NodeCache = require('node-cache');
const config = require('config');
const mysqlPool = require('./mysql_pool');
const promise = require("bluebird");
const logger = require('./logger');
promise.promisifyAll(NodeCache.prototype);
const myCache = new NodeCache(config.get('cache'));

let translationsCachePromise = null;

// will be called many times
function initializeTranslationsCache() {
  if (translationsCachePromise) {
    return translationsCachePromise;
  } else {
    return (translationsCachePromise = actuallyInitializeTranslationsCache());
  }
}

// will be called only once
function actuallyInitializeTranslationsCache() {
  logger.info("Initializing Translations cache.");
  return getTranslations().then((result) => {
    let cacheObject = {};
    result.forEach((el) => cacheObject[el.CACHE_KEY] = el.TRANSLATION);
    myCache.set("translations", cacheObject);
    logger.info("Translations cache was succesfully initialized.");
    return cacheObject;
  });
}

function getTranslations() {
  let translationQuery = "SELECT CONCAT(LANGUAGE,'.',INVOICE_KEY,'.',INVOICE_VALUE) AS CACHE_KEY, TRANSLATION FROM TRANSLATIONS;";

  return promise.using(mysqlPool.getConnection(), (connection) => {
    return connection.queryAsync(translationQuery);
  })
  .catch((err) => logger.error(`Couldn't execute SQL query: ${err.stack}`));
}

function resetTranslationCache() {
  logger.info("Deleting translation cache");
  myCache.del("translations");
  translationsCachePromise = null;
  return initializeTranslationsCache();
}

function get(key) {
  return myCache.getAsync(key);
}

myCache.on("expired", (key, value) => {
  if (key == 'translations') {
    translationsCachePromise = null;
    logger.info("Translations cache expired. Resetting it.");
    return initializeTranslationsCache();
  }
  return false;
});

module.exports = {
  get: get,
  initializeTranslationsCache: initializeTranslationsCache,
  resetTranslationCache: resetTranslationCache
};
