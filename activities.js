"use strict";

var os = require("os");

var async = require("async");

var swfr = require("./index");

var activityHandler = function(i) {
  return function(task, callback) {
    console.log("worker #%d:", i, task);

    switch (task.activityType.name) {
    case "SplitMergeActivity.noop":
      // if (Math.random() < 0.5) {
      //   return callback(new Error("Synthetic error for input: " + task.input[0]));
      // }

      return callback(null, task.input[0]);
    case "SplitMergeActivity.report_result":
      return callback(null, task.input[0]);
    default:
      return callback(new Error("Unsupported activity type: " + task.activityType.name));
    }
  };
};

async.times(os.cpus().length, function(i) {
  return swfr({
    domain: "SplitMerge",
    taskList: "splitmerge_activity_tasklist"
  }, activityHandler(i));
}, function(err, workers) {
  if (err) {
    throw err;
  }

  process.on("SIGTERM", function() {
    return workers.forEach(function(w) {
      return w.cancel();
    });
  });
});
