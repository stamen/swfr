"use strict";

var path = require("path"), 
    util = require("util");

var debug = require("debug")("swfr:decider_context"),
    Promise = require("bluebird"),
    humanize = require("humanize-plus");

var activities = require("./activities");

var SyncDeciderContext = function(task, activitiesFolder) {
  this.task = task;
  this.result = null;
  this.userData = {};
  this.lookup = activities(activitiesFolder);
};

SyncDeciderContext.prototype.activity = function() {
  var options = {
    // defaults go here
    retries: 1
  };

  var runActivity = function(name, version) {
    var args = Array.prototype.slice.call(arguments, 2),
        context = this;

    return new Promise(function(resolve, reject) {
      var activity = context.lookup(name, version);

      if (!activity) {
        return reject(new Error(util.format("Unable to locate activity: %s@%s", name, version)));
      }

      return activity.apply(null, args.concat(function(err, result) {
        if (err) {
          return reject(err);
        }

        var resultLength = JSON.stringify(result).length;

        debug("attribute size: %s", humanize.fileSize(resultLength));

        // SWF limit is 32kb for attributes; warn at 30kb
        if (resultLength >= 30720) {
          console.warn("Result from %s (%s) is > 30kb: %s", name, version, humanize.fileSize(resultLength));
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
  console.log.apply(this, arguments);
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
