"use strict";

var os = require("os"),
    util = require("util");

var async = require("async"),
    debug = require("debug")("swfr:activity"),
    humanize = require("humanize-plus");

var activities = require("./activities");

var activityHandler = function activityHandler(activitiesFolder, id) {
  id = id || 1;

  var lookup = activities(activitiesFolder);

  return function(task, callback) {
    console.log("worker #%d:", id, task);
    var name = task.activityType.name,
        version = task.activityType.version,
        activity = lookup(name, version);

    if (!activity) {
      return callback(new Error(util.format("Unable to locate activity: %s@%s", name, version)));
    }

    return activity.apply(null, task.input.concat(callback));
  };
};

module.exports = activityHandler;
