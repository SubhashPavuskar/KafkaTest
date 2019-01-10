/* jshint strict:true */
/* jshint node: true */
'use strict';

const _ = require('lodash');
const mysqlPool = require('./helpers/mysql_pool');
const promise = require("bluebird");
const config = require('config');
const logger = require('./helpers/logger');
const cache = require('./helpers/cache');
 
function execute(query) {
  return promise.using(mysqlPool.getConnection(), (connection) => {
    return connection.queryAsync({sql: query, typeCast: function (field, next) {
      if (field.type == 'TINY' && field.length == 1) {
        return field.string() == '1';
      }
      return next();
    }}).then((rows) => rows[0]);
  }).catch((err) => logger.error(`Couldn't execute SQL query: ${err.stack}`));
}

function executeQueryForMultipleRecords(query) {
  return promise.using(mysqlPool.getConnection(), (connection) => {
    return connection.queryAsync({sql: query, typeCast: function (field, next) {
      if (field.type == 'TINY' && field.length == 1) {
        return field.string() == '1';
      }
      return next();
    }}).then((rows) => rows);
  }).catch((err) => logger.error(`Couldn't execute SQL query: ${err.stack}`));
}

function getMultipleTranslations(language, translationKeysValuesAndPaths) {
  return cache.get("translations").then((translations) => {
    if (translations) {
      return actuallyGetMultipleTranslations(language, translationKeysValuesAndPaths, translations);
    }
    else {
      return cache.initializeTranslationsCache().then((translations) => actuallyGetMultipleTranslations(language, translationKeysValuesAndPaths, translations));
    }
  });
}

// private

function actuallyGetMultipleTranslations(language, translationKeysValuesAndPaths, translations) {
  let toFetchFromDB = [];
  let translationsArray = translationKeysValuesAndPaths.map((translation) => {
    let path = translation.path;
    let jsonKey = translation.jsonKey;
    let jsonValue = translation.jsonValue;
    let languageKey = [language, jsonKey, jsonValue].join('.');
    let defaultLanguageKey = ["00", jsonKey, jsonValue].join('.');
    if (translations[languageKey]) {
      return({path: path, translation: translations[languageKey]});
    }
    else {
      if (config.get("application.log.translationWarnings"))
        logger.info(`Can't find translation for ${languageKey} in Translations cache`);

      if (translations[defaultLanguageKey]) {
        return({path: path, translation: translations[defaultLanguageKey]});
      }
      else {
        if (config.get("application.log.translationWarnings"))
          logger.warning(`Can't find default translation for ${[jsonKey, jsonValue].join('.')} in Translations cache`);

        toFetchFromDB.push({path: path, jsonKey: jsonKey, jsonValue: jsonValue});
        logger.info("Returning empty from and to featch from DB Array - ",toFetchFromDB);
        return;
      }
    }
  });

  if (toFetchFromDB.length === 0) {
    return translationsArray;
  } else {
    return getMultipleDefaultTranslationsFromDB(toFetchFromDB).then((dbTranslations) => _.compact(translationsArray).concat(dbTranslations));
  }
}

// private methods

function getMultipleDefaultTranslationsFromDB(jsonKeyValuePaths) {
  logger.warning("Getting default translations from DB");
  let requests = jsonKeyValuePaths.map((keyValuePath) => {
    let defaultTranslationQuery = "SELECT TRANSLATION FROM TRANSLATIONS WHERE LANGUAGE = '00' AND INVOICE_KEY = ? AND INVOICE_VALUE = ?;";

    return promise.using(mysqlPool.getConnection(), (connection) => {
      return connection.queryAsync(defaultTranslationQuery, [keyValuePath.jsonKey, keyValuePath.jsonValue]).then((rows) => {
        if(rows[0]){
          return {path: keyValuePath.path, translation: rows[0].TRANSLATION};
        }
        else {
          logger.warning(`Can't find default translation for ${[keyValuePath.jsonKey, keyValuePath.jsonValue].join('.')} in DB`);
          return {path: keyValuePath.path, translation: undefined};
        }
      });
    })
    .catch((err) => logger.error(`Couldn't execute SQL query: ${err.stack}`));
  });

  return promise.all(requests);
}

module.exports = {
  execute: execute,
    executeQueryForMultipleRecords: executeQueryForMultipleRecords,
  getMultipleTranslations: getMultipleTranslations
};
