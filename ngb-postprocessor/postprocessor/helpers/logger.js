/* jshint strict:true */
/* jshint node: true */

require('rconsole');
const config = require('config');

console.set(config.get('log'));

module.exports = console;
