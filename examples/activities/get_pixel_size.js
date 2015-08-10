"use strict";

var url = require("url"),
    util = require("util");

var shell = require("../../lib/shell");

/**
 * Fetch the pixel size for a raster.
 *
 * @returns [horizontal size, vertical size]
 */
module.exports = function getPixelSize(uri, callback) {
  uri = url.parse(uri);

  var args = [
    util.format("http://s3.amazonaws.com/%s%s", uri.hostname, uri.pathname)
  ];

  return shell("gdalinfo", args, {}, function(err, stdout) {
    if (err) {
      return callback(err);
    }

    var size = stdout.split("\n").filter(function(line) {
      return line.match(/Pixel Size/);
    })
    .map(function(line) {
      return line.replace(/^.+\(([-\d\.]+),([-\d\.]+).+$/, "$1 $2").split(" ");
    })
    .pop()
    .map(Number)
    .map(Math.abs);

    return callback(null, size);
  });
};

module.exports.version = "1.0";
