"use strict";

var execFile = require("child_process").execFile;

var debug = require("debug"),
    holdtime = require("holdtime");

var log = debug("swfr_shell");

module.exports = function(cmd, args, options, callback) {
  options.timeout = options.timeout || 15 * 60e3;

  var child = execFile(cmd, args, options, holdtime(function(err, stdout, stderr, elapsed) {
    log("%s: %dms", cmd, elapsed);

    if (err) {
      return callback(err);
    }

    return callback(null, stdout, stderr);
  }));

  if (debug.enabled("swfr_shell")) {
    child.stdout.pipe(process.stdout);
    child.stderr.pipe(process.stderr);
  }

  var alive = true,
      exitListener = function() {
        if (alive) {
          child.kill("SIGKILL");
        }
      };

  // terminate child processes when the runner is asked to quit; successfully
  // completed processes will carry on and upload their output
  ["SIGINT", "SIGTERM"].forEach(function(signal) {
    process.once(signal, exitListener);
  });

  // mark the child as dead when it exits
  child.on("exit", function() {
    alive = false;

    // TODO remove event listeners
  });
};
