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
      // var fd = fs.openSync(fileList, 'w');
      // var len = files.length;
      // for(var i = 0; i < len; i++) {
      //   fs.writeSync(fd, files[i] + "\n");
      // }
      // fs.closeSync(fd);

      var list = fs.createWriteStream(fileList, {
        encoding: "utf8"
      }).on("finish", function() {
        var args = [
          "-input_file_list", fileList,
          localOutputPath
        ];

        return shell("gdalbuildvrt", args, {}, function(err) {
          fs.unlink(fileList, function() {});
          done(err);
        });
      });

      files.forEach(function(f) {
        list.write(f + "\n");
      });

      return list.end();
    });
  });
};

module.exports.version = "1.0";
