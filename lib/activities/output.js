"use strict";

var fs = require("fs"),
    path = require("path"),
    url = require("url");

var debug = require("debug"),
    mkdirp = require('mkdirp');

var s3Output = require("./s3_output");

var log = debug("swfr:upload");

module.exports = function(outputUri, done, fn) {
  if(outputUri.indexOf("s3://") === 0) {
    return s3Output(outputUri, done, fn);
  } 

  // Local output
  
  var call = function() {
    return fn(null, outputUri, function(err, save) {
      if (err) {
        // wrapped function failed; propagate the error
        return done(err);
      }

      return done(null, outputUri);
    });
  };

  var d = path.dirname(outputUri);
  return fs.exists(d, function(exists) {
    if(!exists) {
      return mkdirp(d, function (err) {
        if (err) {
          return done(err);
        }

        return call();
      });
    }

    return call();
  });
};
