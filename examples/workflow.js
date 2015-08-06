"use strict";

var os = require("os"),
    util = require("util");

var Promise = require("bluebird");

var decider = require("./decider");

console.log("swfr@%s:%d", os.hostname(), process.pid);

var worker = decider({
  domain: "SplitMerge",
  taskList: "splitmerge_workflow_tasklist"
}, function(input) {
  var context = this;

  this.log("Running workflow");
  this.log("Input:", input);

  var values = [];

  for (var x = 0; x < 1100; x++) {
    values.push(x);
  }

  // TODO stick map in the context to manage concurrency?
  // or just reiterate the fact that a workflow can have no more than 100
  // activity tasks active at a time?
  var calls = Promise.map(values, function(i) {
    context.status = util.format("SplitMergeActivity.noop@1.4:", i);

    return context.activity({
      retries: 5
      // TODO options (heartbeatTimeout, etc.)
    })("SplitMergeActivity.noop", "1.4", i);
  }, {
    concurrency: 100
  });

  return Promise.all(calls)
    .bind(context)
    .then(function(result) {
      this.log("Output:", result);

      // TODO cache the result of this function
      // TODO checkpoints--save input to checkpoint and skip everything before
      // it (saves on pagination, saves on computation)
      // checkpoints need to wrap execution up to a certain point so that it
      // can be bypassed
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
