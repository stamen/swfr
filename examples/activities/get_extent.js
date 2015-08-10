"use strict";

var debug = require("debug"),
    url = require("url"),
    util = require("util");

var shell = require("../../lib/shell");

var log = debug("swfr:shell");

// Regex to extract coordinates.
var COORDINATE_REGEX = /([\[\(])([^,]*),(.*?)([\]\)])/;

/**
 * Fetch the extent of a raster.
 *
 * @returns [upper left, lower right]
 */
module.exports = function getExtent(uri, callback) {
  var args = [
    "-nofl", // In case this is a VRT with too many files to be caught in stdout
    uri
  ];

  return shell("gdalinfo", args, {}, function(err, stdout) {
    if (err) {
      return callback(err);
    }

    var extent = stdout.split("\n").filter(function(line) {
      return line.match(/Upper Left|Lower Right/);
    })
    .map(function(line) {
      var m = line.match(COORDINATE_REGEX),
          x = +m[2],
          y = +m[3];

      return [x, y];
    });

    log("Got extent: %s", extent);
    return callback(null, extent);
  });
};

module.exports.version = "1.0";
