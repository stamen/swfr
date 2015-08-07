"use strict";

var assert = require("assert");

var clone = require("clone"),
    gdal = require("gdal");

var output = require("./output"),
    shell = require("./shell");

module.exports = function translate(localInputPath, outputUri, options, callback) {
  return output(outputUri, callback, function(err, localOutputPath, done) {
    if (err) {
      return callback(err);
    }

    var args = [];
    if (options.outputFormat) {
      args = args.concat(["-of", options.outputFormat]);
    }

    if (options.bands) {
      var len = options.bands.length;
      for (var i = 0; i < len; i++) {
        args = args.concat(["-b", "" + options.bands[i]]);
      }
    }

    if ("nodata" in options) {
      args = args.concat(["-a_nodata", options.nodata]);
    }

    args = args.concat([
      localInputPath,
      localOutputPath
    ]);

    var env = clone(process.env);

    env.GDAL_CACHEMAX = 256;
    env.GDAL_DISABLE_READDIR_ON_OPEN = true;
    env.CHECK_WITH_INVERT_PROJ = true; // handle -180/180, 90/-90 correctly
    env.CPL_VSIL_CURL_ALLOWED_EXTENSIONS = ".tiff,.zip,.vrt";

    return shell("gdal_translate", args, {
      env: env,
      timeout: 10 * 60e3 // 10 minutes
    }, done);
  });
};

module.exports.version = "1.0";
