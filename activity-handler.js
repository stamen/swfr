"use strict";

var os = require("os"),
    util = require("util");

var async = require("async"),
    debug = require("debug")("swfr:activity"),
    humanize = require("humanize-plus");

var activity = require("./activity"),
    activities = require("./lib/activities");

var activityHandler = function activityHandler(activitiesFolder, id) {
  id = id | 1;

  var taskContext = this;
  var lookup = activities(activitiesFolder);

  return function(task, callback) {
    console.log("worker #%d:", id, task);
    var name = task.activityType.name,
        version = task.activityType.version,
        activity = lookup(name, version);

    if (!activity) {
      return callback(new Error(util.format("Unable to locate activity: %s@%s", name, version)));
    }

    return activity.apply(taskContext, task.input.concat(function(err, result) {
      if (err) {
        return callback(err);
      }

      var resultLength = JSON.stringify(result).length;

      debug("attribute size: %s", humanize.fileSize(resultLength));

      // SWF limit is 32kb for attributes; warn at 30kb
      if (resultLength >= 30720) {
        console.warn("Result from %s (%s) is > 30kb: %s", name, version, humanize.fileSize(resultLength));
      }

      return callback(null, result);
    }));
  };
};

module.exports = activityHandler;
