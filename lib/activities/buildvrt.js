"use strict";

var fs = require('fs'),
    path = require('path');

var tmp = require('tmp');

var output = require("./output"),
    shell = require("./shell");

module.exports = function buildVRT(files, outputUri, callback) {
  return output(outputUri, callback, function(err, localOutputPath, done) {
    if (err) {
      return callback(err);
    }

    if (files.length === 0) {
      return done(new Error("No files provided for " + localOutputPath));
    }

    return tmp.tmpName({
      postfix: ".txt"
    }, function(err, fileList) {
      if (err) {
        return done(err);
      }

      var stream = fs.createWriteStream(fileList, {
        encoding: "utf8"
      });
        
      stream.on("finish", function() {
        var args = [
          "-input_file_list", fileList,
          localOutputPath
        ];

        return shell("gdalbuildvrt", args, {}, function(err) {
          fs.unlink(fileList, function() {});
          return done(err);
        });
      });
        
      stream.on("error", function (err) {
        fs.unlink(fileList, function() {});
        return done(err);
      });
      
      files.forEach(function(f) {
        stream.write(f + "\n");
      });

      return stream.end();
    });
  });
};

module.exports.version = "1.0";
