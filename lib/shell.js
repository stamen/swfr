"use strict";

var execFile = require("child_process").execFile;

var debug = require("debug"),
    holdtime = require("holdtime");

var log = debug("swfr:shell");

module.exports = function(cmd, args, options, callback) {
  options.timeout = options.timeout || 15 * 60e3;

  log("%s %s", cmd, args.join(" "));

  var child = execFile(cmd, args, options, holdtime(function(err, stdout, stderr, elapsed) {
    log("%s: %dms", cmd, elapsed);

    if (err) {
      return callback(err);
    }

    return callback(null, stdout, stderr);
  }));

  if (debug.enabled("swfr:shell")) {
    child.stdout.pipe(process.stdout);
    child.stderr.pipe(process.stderr);
  }

  var alive = true,
      exitListener = function exitListener() {
        if (alive) {
          child.kill("SIGKILL");
        }
      };

  process.setMaxListeners(0);

  // terminate child processes when the runner is asked to quit; successfully
  // completed processes will carry on and upload their output
  ["SIGINT", "SIGTERM"].forEach(function(signal) {
    process.once(signal, exitListener);
  });

  // mark the child as dead when it exits
  child.on("exit", function() {
    alive = false;

    // remove event listeners
    ["SIGINT", "SIGTERM"].forEach(function(signal) {
      process.removeListener(signal, exitListener);
    });
  });
};
