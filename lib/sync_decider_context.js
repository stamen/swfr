"use strict";

var P = require("bluebird");

var lookup = require("./activities");

var SyncDeciderContext = function(task) {
  this.task = task;
  this.result = null;
  this.userData = {};
};

SyncDeciderContext.prototype.activity = function() {
  var options = {
    // defaults go here
    retries: 1
  };

  var runActivity = function(name, version) {
    var args = Array.prototype.slice.call(arguments, 2),
        context = this;

    return new P(function(resolve, reject) {
      var activity = lookup(name, version);

      if (!activity) {
        return reject(new Error("Unable to locate activity: %s@%s", name, version));
      }

      return activity.apply(context, args.concat(function(err, result) {
        if (err) {
          return reject(err);
        }

        return resolve(result);
      }));
    });
  };

  if (typeof arguments[0] === "object") {
    var providedOptions = arguments[0];

    // copy options
    Object.keys(providedOptions).forEach(function(k) {
      options[k] = providedOptions[k];
    });

    return runActivity;
  }

  return runActivity.apply(this, arguments);
};

SyncDeciderContext.prototype.complete = function(result) {
  this.result = result;
};

SyncDeciderContext.prototype.log = function() {
  console.log.apply(null, arguments);
};

Object.defineProperty(SyncDeciderContext.prototype, "status", {
  get: function() {
    return this._status;
  },
  set: function(status) {
    this._status = status;

    console.log("STATUS:", status);
  }
});

module.exports = SyncDeciderContext;
