"use strict";

var url = require("url"),
    util = require("util");

var shell = require("./shell");

/**
 * Fetch the extent of a raster.
 *
 * @returns [upper left, lower right]
 */
module.exports = function getExtent(uri, callback) {
  uri = url.parse(uri);

  var args = [
    util.format("http://s3.amazonaws.com/%s%s", uri.hostname, uri.pathname)
  ];

  return shell("gdalinfo", args, {}, function(err, stdout) {
    if (err) {
      return callback(err);
    }

    var extent = stdout.split("\n").filter(function(line) {
      return line.match(/Upper Left|Lower Right/);
    })
    .map(function(line) {
      return line.replace(/^.+\(([-\d\.]+),\s*([-\d\.]+).+$/, "$1 $2").split(" ");
    })
    .map(function(pair) {
      return pair.map(Number);
    });

    return callback(null, extent);
  });
};

module.exports.version = "1.0";
