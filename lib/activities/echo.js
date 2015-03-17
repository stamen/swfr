"use strict";

module.exports = function echo(input, callback) {
  return callback(null, input);
};

module.exports.version = "1.0";
