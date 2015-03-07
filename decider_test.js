"use strict";

var os = require("os"),
    util = require("util");

var Promise = require("bluebird");

var decider = require("./decider");

console.log("swfr@%s:%d", os.hostname(), process.pid);

var worker = decider({
  domain: "SplitMerge",
  taskList: "splitmerge_workflow_tasklist"
}, function(payload) {
  var context = this;

  // console.log("payload:", payload);
  // console.log("context:", context);


  // TODO this always outputs because we're both replaying and not replaying at
  // this point
  this.log("Running workflow");

  // TODO stick map in the context to manage concurrency?
  var calls = Promise.map([0, 1, 2, 3, 4], function(i) {
    context.status = util.format("SplitMergeActivity.noop@1.4:", i);

    return context.activity({
      retries: 5
      // TODO options (heartbeatTimeout, etc.)
    })("SplitMergeActivity.noop", "1.4", i);
  }, {
    concurrency: 2
  });

  return Promise.all(calls)
    .bind(context)
    .then(function(result) {
      // TODO the runner thinks we're retrying at this point
      this.log("Output:", result);

      var sum = result.reduce(function(a, b) {
        return a + b;
      }, 0);

      this.log("Sum:", sum);

      this.status = "print sum";
      return this.activity({ retries: 5 })("SplitMergeActivity.noop", "1.4", sum);
    })
    .then(function(result) {
      this.log("Result:", result);

      this.status = "print done";
      return this.activity({ retries: 5 })("SplitMergeActivity.noop", "1.4", "done");
    })
    .then(function() {
      this.log("done.");

      this.status = "done";
      this.complete("done");
    });
});

process.on("SIGTERM", function() {
  worker.cancel();
});
