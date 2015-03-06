"use strict";
var swfr = require("./index");

var worker = swfr({
  domain: "SplitMerge",
  taskList: "splitmerge_activity_tasklist"
}, function(task, callback) {
  console.log("worker #1:", task);

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
});

process.on("SIGTERM", function() {
  worker.cancel();
});
