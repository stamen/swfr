"use strict";

var assert = require("assert");

var clone = require("clone");

var output = require("../../lib/output"),
    shell = require("../../lib/shell");

module.exports = function resample(localInputPath, outputUri, options, callback) {
  try {
    assert.ok(Array.isArray(options.targetExtent), "resample: targetExtent must be an array");
    assert.equal(4, options.targetExtent.length, "resample: targetExtent must be an array of 4 elements");
  } catch (err) {
    return callback(err);
  }

  return output(outputUri, callback, function(err, localOutputPath, done) {
    if (err) {
      return callback(err);
    }

    // TODO: Shouldn't assume target SRS.
    var args = [
      "-q",
      "-te", options.targetExtent[0], options.targetExtent[1], options.targetExtent[2], options.targetExtent[3],
      "-wm", 256, // allow GDAL to work with larger chunks (diminishing returns after 500MB, supposedly)
      "-wo", "NUM_THREADS=ALL_CPUS",
      "-multi",
      "-co", "tiled=yes",
      "-r", "bilinear"
    ];

    if (options.targetSrs) {
      args = args.concat([
        "-t_srs", options.targetSrs
      ]);
    }

    if (options.targetResolution) {
      assert.ok(Array.isArray(options.targetResolution), "resample: targetResolution must be an array");
      assert.equal(2, options.targetResolution.length, "resample: targetResolution must be an array of 2 elements");
      args = args.concat([
        "-tr", options.targetResolution[0], options.targetResolution[1]
      ]);
    }
    
    if (options.targetSize) {
      assert.ok(Array.isArray(options.targetSize), "resample: targetSize must be an array");
      assert.equal(2, options.targetSize.length, "resample: targetSize must be an array of 2 elements");
      args = args.concat([
        "-ts", options.targetSize[0], options.targetSize[1]
      ]);
    }

    if (options.alignPixels) {
      args.push("-tap");
    }

    if (!options.nocompression) {
      args = args.concat([
        "-co", "compress=lzw",
        "-co", "predictor=2"
      ]);
    }

    if (options.overwrite) {
      args.push("-overwrite");
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

    return shell("gdalwarp", args, {
      env: env,
      timeout: 10 * 60e3 // 10 minutes
    }, function(err) {
      if (err) {
      return done.apply(null, arguments);
      }

      // TODO: We want a way to filter out tiles that are strictly filled with NoData.
      // Below is an attempt at doing that, but it's very slow. What's a better way
      // to do this?
      //
      // var ds = gdal.open(localOutputPath);
      // try {
      //  gdal.open(localOutputPath).bands.get(1).getStatistics(false, true);
      // } catch (err) {
      //   // likely "Failed to compute statistics, no valid pixels found in
      //   // sampling.", which means that it's all NODATA, and thus not worth
      //   // saving

      //   return done();
      // }
      // try {
      //   ds.bands.get(1).getStatistics(false, true);
      // } catch (err) {
      //   // likely "Failed to compute statistics, no valid pixels found in
      //   // sampling.", which means that it's all NODATA, and thus not worth
      //   // saving

      //   return done();
      // } finally {
      //   ds.close();
      // }

      return done.apply(null, arguments);
    });
  });
};

module.exports.version = "1.0";
