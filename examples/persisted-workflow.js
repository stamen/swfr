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

      for (var x = 0; x < 50000; x++) {
        values.push(x);
      }

      return this.activity({ retries: 5 })(ACTIVITY_NAME, ACTIVITY_VERSION, values);
    })
    .then(function() {
      this.log("done");
      this.complete("done");
    });
});

process.on("SIGTERM", function() {
  worker.cancel();
});
