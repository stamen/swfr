"use strict";

var path = require("path"),
    url = require("url"),
    util = require("util");

var mercator = new (require("sphericalmercator"))();

var decider = require("./decider");

var CELL_PADDING = 1,
    CELL_HEIGHT = 1024,
    CELL_WIDTH = CELL_HEIGHT,
    // WGS 84 semi-major axis (m)
    SEMI_MAJOR_AXIS = 6378137;

var worker = decider({
  sync: true,
  domain: "SplitMerge",
  taskList: "splitmerge_workflow_tasklist"
}, function(chain, input) {
  var bucket = input.bucket,
      targetZoom = input.zoom, // nearest integral zoom (log_2(max(pixel size(m))))
      prefix = "source/",
      rawPrefix = "4326/",
      webPrefix = "3857/";

  return chain
    .then(function() {
      // list files in bucket

      this.status = "listBucket";
      return this.activity("listBucket", "1.0", util.format("s3://%s/%s", bucket, prefix));
    })
    .then(function(keys) {
      // filter for files we can handle

      return keys.filter(function(key) {
        return key.match(/\.zip$/);
      });
    })
    .then(function(keys) {
      // only deal with a single file for now
      return keys.slice(0, 1);
    })
    .map(function(key) {
      // rewrite each as a GeoTIFF

      var basename = path.basename(key, ".zip"),
          input = util.format("/vsizip/vsicurl/http://s3.amazonaws.com/%s/%s/%s.tif", bucket, key, basename),
          output = util.format("s3://%s/%s%s.tiff", bucket, rawPrefix, basename);

      this.status = util.format("Reprojecting %s to 4326", key);
      return this.activity("reproject", "1.0", input, output, {
        targetSRS: "EPSG:4326",
        srcNoData: -32768,
        dstNoData: -32768
      });
    }, {
      concurrency: 5 // TODO if local, limit to os.cpus().length, otherwise 100 (or fewer)
    })
    .then(function(keys) {
      // generate VRT from results
      // TODO combine list + VRT step into a single activity

      var output = util.format("s3://%s/cgiar-csi-srtm-4326.vrt", bucket),
          files = keys.map(function(key) {
            var uri = url.parse(key);

            return util.format("/vsicurl/http://s3.amazonaws.com/%s%s", uri.hostname, uri.pathname);
          });

      return this.activity("buildVRT", "1.0", files, output);
    })
    .then(function(vrt) {
      // get extent

      return this.activity("getExtent", "1.0", vrt);
    })
    .then(function(extent) {
      // create cells at nearest integral zoom

      extent = extent.map(mercator.forward);

      var cells = [],
          widthPx = Math.pow(2, targetZoom + 8), // world's width in px, assuming 256x256 tiles
          heightPx = widthPx,
          // 2 * pi * earth radius * cos(lat)
          circumference = 2 * Math.PI * SEMI_MAJOR_AXIS * Math.cos(0),
          // extents
          minX = (circumference / 2) * -1,
          minY = minX,
          maxX = (circumference / 2),
          maxY = maxX,
          // circumference / pixel width(zoom)
          targetResolution = circumference / widthPx,
          width = CELL_WIDTH * targetResolution,
          height = width,
          // human-readable extent components
          left = extent[0][0],
          right = extent[1][0],
          bottom = extent[1][1],
          top = extent[0][1];

      // chop the (overlapping) world into cells
      for (var yi = 0; yi < heightPx / CELL_HEIGHT; yi++) {
        var y = heightPx / CELL_HEIGHT - yi - 1, // convert from TMS to XYZ coords (top-left origin)
            y1 = Math.max(minY, (yi * height) - (circumference / 2) - (CELL_PADDING * targetResolution)),
            y2 = Math.min(maxY, ((yi + 1) * height) - (circumference / 2) + (CELL_PADDING * targetResolution));

        for (var xi = 0; xi < widthPx / CELL_WIDTH; xi++) {
          var x1 = Math.max(minX, (xi * width) - (circumference / 2) - (CELL_PADDING * targetResolution)),
              x2 = Math.min(maxX, ((xi + 1) * width) - (circumference / 2) + (CELL_PADDING * targetResolution));
          // check intersection
          if (((left <= x1 && x1 <= right) ||
               (left <= x2 && x2 <= right)) &&
              ((bottom <= y1 && y1 <= top) ||
               (bottom <= y2 && y2 <= top))) {
            cells.push({
              source: "/vsicurl/http://s3.amazonaws.com/cgiar-csi-srtm.openterrain.org/cgiar-csi-srtm-4326.vrt",
              target: util.format("s3://%s/%sz%d/%d/%d.tiff", bucket, webPrefix, targetZoom, xi, y),
              options: {
                targetExtent: [x1, y1, x2, y2],
                targetResolution: [targetResolution, targetResolution]
              }
            });
          }
        }
      }

      this.log("Generating %d cells.", cells.length);

      return cells;
    })
    .map(function(cell) {
      return this.activity("resample", "1.0", cell.source, cell.target, cell.options);
    }, {
      concurrency: 5 // TODO if local, limit to os.cpus().length, otherwise 100 (or fewer)
    })
    .filter(function(cell) {
      return !!cell;
    })
    .then(function(tiles) {
      // TODO warn if JSONified input >= 4k
      // if that's the case, compose list + VRT in a single activity
      this.log("Generated tiles:", tiles);
    })
    .then(function() {
      this.complete();
    });

  // generate hillshade from each cell
  // generate VRT from cells
  // generate VRT from hillshade cells
  //
  // get extent
  // create cells at overview zoom
  // generate hillshade from each overview cell
  // generate VRT from overview cells
  // generate VRT from overview hillshade cells
  // repeat
});

worker.start({
  bucket: "cgiar-csi-srtm.openterrain.org",
  zoom: 12 // pre-determined
});

process.on("SIGTERM", function() {
  worker.cancel();
});
