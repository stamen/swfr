"use strict";

var assert = require("assert");

var clone = require("clone");

var s3Output = require("./s3_output"),
    shell = require("./shell");

module.exports = function reproject(input, output, options, callback) {
  try {
    assert.ok(options.targetSRS, "reproject: Target SRS is required");
  } catch (err) {
    return callback(err);
  }

  return s3Output(output, callback, function(err, outputFilename, done) {
    if (err) {
      return callback(err);
    }

    var args = [
      "-q",
      "-t_srs", options.targetSRS,
      "-wo", "NUM_THREADS=ALL_CPUS",
      "-multi",
      "-co", "tiled=yes",
      "-co", "compress=lzw",
      "-co", "predictor=2",
      "-r", "bilinear",
      input,
      outputFilename
    ];

    if (options.srcNoData != null) {
      args.unshift("-srcnodata", options.srcNoData);
    }

    if (options.dstNoData != null) {
      args.unshift("-dstnodata", options.dstNoData);
    }

    var env = clone(process.env);

    env.GDAL_CACHEMAX = 256;
    env.GDAL_DISABLE_READDIR_ON_OPEN = true;
    env.CHECK_WITH_INVERT_PROJ = true; // handle -180/180, 90/-90 correctly
    env.CPL_VSIL_CURL_ALLOWED_EXTENSIONS = ".tiff,.zip,.vrt";

    return shell("gdalwarp", args, {
      env: env,
      timeout: 10 * 60e3 // 10 minutes
    }, done);
  });
};

module.exports.version = "1.0";
