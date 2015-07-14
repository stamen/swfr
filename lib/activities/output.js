"use strict";

var fs = require("fs"),
    path = require("path"),
    url = require("url");

var debug = require("debug"),
    mkdirp = require("mkdirp");

var s3Output = require("./s3_output");

module.exports = function(outputUri, done, fn) {
  var uri = url.parse(outputUri);

  if (uri.protocol === "s3:") {
    return s3Output(outputUri, done, fn);
  }

  // Local output

  var d = path.dirname(outputUri);
  return mkdirp(d, function (err) {
    if (err) {
      return done(err);
    }

    return fn(null, outputUri, function(err, save) {
      if (err) {
        // wrapped function failed; propagate the error
        return done(err);
      }

      return done(null, outputUri);
    });
  });
};
