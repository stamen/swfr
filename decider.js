"use strict";

var assert = require("assert"),
    EventEmitter = require("events").EventEmitter,
    os = require("os"),
    stream = require("stream"),
    util = require("util");

var _ = require("highland"),
    AWS = require("aws-sdk");

var DeciderWorker = require("./lib/DeciderWorker"),
    SyncDeciderWorker = require("./lib/SyncDeciderWorker");

// TODO this overrides any file-based configuration that may have occurred
AWS.config.update({
  region: process.env.AWS_DEFAULT_REGION || "us-east-1"
});

var swf = new AWS.SWF();

/**
 * Available options:
 * * domain - Workflow domain (required)
 * * taskList - Task list (required)
 */
// TODO Distributor
module.exports = function(options, fn) {
  if (options.sync) {
    var worker = new EventEmitter();

    var source = new stream.PassThrough({
      objectMode: true
    });

    source.pipe(new SyncDeciderWorker(fn));

    worker.cancel = function() {
      source.end();
    };

    worker.start = function(input) {
      source.write({
        input: input
      });
    };

    return worker;
  }

  assert.ok(options.domain, "options.domain is required");
  assert.ok(options.taskList, "options.taskList is required");

  var worker = new EventEmitter();

  var getEvents = function(events, nextPageToken, callback) {
    if (!nextPageToken) {
      return setImmediate(callback, null, events);
    }

    return swf.pollForDecisionTask({
      domain: options.domain,
      taskList: {
        name: options.taskList
      },
      identity: util.format("swfr@%s:%d", os.hostname(), process.pid),
      nextPageToken: nextPageToken
    }, function(err, data) {
      if (err) {
        return callback(err);
      }

      if (data.nextPageToken) {
        return getEvents(events.concat(data.events), data.nextPageToken, callback);
      }

      return callback(null, events.concat(data.events));
    });
  };

  var source = _(function(push, next) {
    // TODO note: activity types need to be registered in order for workflow
    // executions to not fail

    // TODO get events from an LRU cache (which means requesting events in
    // reverse order)
    // in batches of 100 (or whatever the page size is set to)
    var poll = swf.pollForDecisionTask({
      domain: options.domain,
      taskList: {
        name: options.taskList
      },
      identity: util.format("swfr@%s:%d", os.hostname(), process.pid)
    }, function(err, data) {
      if (err) {
        console.warn(err.stack);

        return next();
      }

      if (!data.taskToken) {
        return next();
      }

      return getEvents(data.events, data.nextPageToken, function(err, events) {
        if (err) {
          console.warn(err.stack);

          return next();
        }

        data.events = events;

        push(null, data);

        return next();
      });
    });

    // cancel requests when the stream ends so we're not hanging onto any
    // outstanding resources (swf.pollForDecisionTask waits 60s for messages
    // by default)

    var abort = poll.abort.bind(poll);

    source.on("end", abort);

    // clean up event listeners
    poll.on("complete", _.partial(source.removeListener.bind(source), "end", abort));
  });

  if (fn) {
    source.pipe(new DeciderWorker(fn));

    worker.cancel = function() {
      source.destroy();
    };
  }

  return worker;
};
