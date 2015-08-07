"use strict";

var os = require("os"),
    util = require("util");

var env = require("require-env");

var decider = require("../decider");

var ACTIVITY_NAME = "noop",
    ACTIVITY_VERSION = "1.4";

console.log("swfr@%s:%d", os.hostname(), process.pid);

var worker = decider({
  domain: env.require("AWS_SWF_DOMAIN")
}, function(chain, input) {
  return chain
    .then(function() {
      var values = [];

      for (var x = 0; x < 10; x++) {
      // for (var x = 0; x < 1100; x++) {
        values.push(x);
      }

      return values;
    })
    .map(function(i) {
      this.log("%s:%s:", ACTIVITY_NAME, ACTIVITY_VERSION, i);
      this.status = util.format("%s@%s:", ACTIVITY_NAME, ACTIVITY_VERSION, i);

      return this.activity({
        retries: 5
        // TODO options (heartbeatTimeout, etc.)
      })(ACTIVITY_NAME, ACTIVITY_VERSION, i);
    }, {
      concurrency: 100 // TODO transparently manage concurrency?
    })
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
      return this.activity({ retries: 5 })(ACTIVITY_NAME, ACTIVITY_VERSION, sum);
    })
    .then(function(result) {
      this.log("Result:", result);

      this.status = "print done";
      return this.activity({ retries: 5 })(ACTIVITY_NAME, ACTIVITY_VERSION, "done");
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
