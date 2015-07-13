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

    if(files.length < 1000) {
      var args = [
        localOutputPath
      ].concat(files);

      return shell("gdalbuildvrt", args, {}, done);
    } else {

      return tmp.tmpName({
        postfix: ".txt"
      }, function(err, fileList) {
        if (err) {
          return done(err);
        }
        var fd = fs.openSync(fileList, 'w');
        var len = files.length;
        for(var i = 0; i < len; i++) {
          fs.writeSync(fd, files[i] + "\n");
        }
        fs.closeSync(fd);
        var args = [
          "-input_file_list", fileList,
          localOutputPath
        ];

        console.log("gdalbuildvrt " + args);
        return shell("gdalbuildvrt", args, {}, function(err) {
          fs.unlink(fileList, function() {});
          done(err);
        });
      });
    }
  });
};

module.exports.version = "1.0";
