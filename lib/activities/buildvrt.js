"use strict";

var s3Output = require("./s3_output"),
    shell = require("./shell");

module.exports = function buildVRT(files, output, callback) {
  return s3Output(output, callback, function(err, outputFilename, done) {
    if (err) {
      return callback(err);
    }

    if (files.length === 0) {
      return done(new Error("No files provided for " + outputFilename));
    }

    var args = [
      outputFilename
    ].concat(files);

    // TODO this will fail if the list is too long
    return shell("gdalbuildvrt", args, {}, done);
  });
};

module.exports.version = "1.0";
