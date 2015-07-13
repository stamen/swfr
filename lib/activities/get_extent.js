"use strict";

var debug = require("debug"),
    url = require("url"),
    util = require("util");

var shell = require("./shell");

var log = debug("swfr:shell");

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

  // Regex to extract coordinates.
  var re = /([\[\(])([^,]*),(.*?)([\]\)])/;

  return shell("gdalinfo", args, {}, function(err, stdout) {
    if (err) {
      return callback(err);
    }

    var extent = stdout.split("\n").filter(function(line) {
      return line.match(/Upper Left|Lower Right/);
    })
    .map(function(line) {
      log(line);
      var m = line.match(re);
      var x = Number(m[2]);
      var y = Number(m[3]);
      return [x, y];
    });
    
    console.log("Got extent: %s", extent);
    return callback(null, extent);
  });
};

module.exports.version = "1.0";
