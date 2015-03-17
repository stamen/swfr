"use strict";

var s3Output = require("./s3_output"),
    shell = require("./shell");

module.exports = function buildVRT(files, output, callback) {
  return s3Output(output, callback, function(err, outputFilename, done) {
    if (err) {
      return callback(err);
    }

    var args = [
      outputFilename
    ].concat(files);

    return shell("gdalbuildvrt", args, {}, done);
  });
};

module.exports.version = "1.0";