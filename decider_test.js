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

  var calls = Promise.map([0, 1, 2, 3, 4], function(i) {
    context.status = util.format("SplitMergeActivity.noop@1.4:", i);

    return context.activity({
      // TODO options (heartbeatTimeout, etc.)
    })("SplitMergeActivity.noop", "1.4", i);

    // TODO support retries
    // // need to bind arguments to fn, as retry doesn't accept them
    // return retry(call.bind(null, "echo", i), {
    //   interval: 1, // this either running for playback or remotely, so waiting does nothing (0 leads to the default value)
    //   max_tries: 5
    // })
    //   .catch(function(err) {
    //     // unwrap cancellation errors and propagate them
    //     if (err.failure && err.failure instanceof Promise.CancellationError) {
    //       throw err.failure;
    //     }
    //
    //     throw err;
    //   });
  }, {
    // concurrency: 2
  });

  return Promise.all(calls)
    .bind(context)
    .then(function(result) {
      this.log("Output:", result);

      var sum = result.reduce(function(a, b) {
        return a + b;
      }, 0);

      this.log("Sum:", sum);

      this.status = "print sum";
      return this.activity("SplitMergeActivity.noop", "1.4", sum);
    })
    .then(function(result) {
      this.log("Result:", result);

      this.status = "print done";
      return this.activity("SplitMergeActivity.noop", "1.4", "done");
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
