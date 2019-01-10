/* jshint strict:true */
/* jshint node: true */

'use strict';

const mysql = require('mysql');
const config = require('config');
const promise = require("bluebird");

promise.promisifyAll(mysql);
promise.promisifyAll(require("mysql/lib/Connection").prototype);
promise.promisifyAll(require("mysql/lib/Pool").prototype);

const pool = mysql.createPool(config.get('mysql'));

function getConnection() {
    return pool.getConnectionAsync().disposer(function(connection) {
        connection.release();
    });
}

module.exports.getConnection = getConnection;
