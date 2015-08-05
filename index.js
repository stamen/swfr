"use strict";

var assert = require("assert"),
    EventEmitter = require("events").EventEmitter,
    https = require("https"),
    os = require("os"),
    stream = require("stream"),
    util = require("util");

var _ = require("highland"),
    AWS = require("aws-sdk");

var decider = require("./decider"),
    payloadPersister = require("./lib/payload-persister");

var agent = new https.Agent({
  // Infinity just boosts the max value; in practice this will be no larger
  // than your configured concurrency (number of workers)
  maxSockets: Infinity
});

AWS.config.update({
  httpOptions: {
    agent: agent
  },
  region: process.env.AWS_DEFAULT_REGION || AWS.config.region || "us-east-1"
});

var swf = new AWS.SWF();

var ActivityWorker = function(fn) {
  stream.Writable.call(this, {
    objectMode: true,
    highWaterMark: 1 // limit the number of buffered tasks
  });

  this._write = function(task, encoding, callback) {
    var heartbeat = function() {
          return swf.recordActivityTaskHeartbeat({
            taskToken: task.taskToken
          }, function(err, data) {
            if (err) {
              console.warn(err.stack);
            }
          });
        };

    // send a heartbeat every 30s
    var extension = setInterval(heartbeat, 30e3);

    // load payload from an external source if necessary
    return payloadPersister.load(task.payload, function(err, payload) {
      if (err) {
        // cancel the reservation extention
        clearInterval(extension);

        console.warn(err.stack);
        return callback();
      }

      // pass the activity type + input to the function and provide the rest as
      // the context
      return fn.call(task, task.payload, function(err, result) {
        // cancel the reservation extension
        clearInterval(extension);

        if (err) {
          console.log("Failed:", err);

          return swf.respondActivityTaskFailed({
            taskToken: task.taskToken,
            reason: err.message,
            details: JSON.stringify({
              payload: task.payload,
              stack: err.stack
            })
          }, function(err, data) {
            if (err) {
              console.warn(err.stack);
            }

            return callback();
          });
        }

        // mark task as complete

        // TODO if result > 32k, save to DynamoDB and replace it with a URI
        // include a timestamp in the item

        return swf.respondActivityTaskCompleted({
          taskToken: task.taskToken,
          result: JSON.stringify(result)
        }, function(err, data) {
          if (err) {
            console.warn(err.stack);
          }

          return callback();
        });
      });
    });
  };
};

util.inherits(ActivityWorker, stream.Writable);

module.exports.decider = decider;

/**
 * Available options:
 * * domain - Workflow domain (required)
 * * taskList - Task list (required)
 */
module.exports.activity = function(options, fn) {
  assert.ok(options.domain, "options.domain is required");
  assert.ok(options.taskList, "options.taskList is required");

  var worker = new EventEmitter();

  var source = _(function(push, next) {
    // TODO note: activity types need to be registered in order for workflow
    // executions to not fail

    var poll = swf.pollForActivityTask({
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

      try {
        var task = {
          domain: options.domain,
          taskList: options.taskList,
          taskToken: data.taskToken,
          activityId: data.activityId,
          startedEventId: data.startedEventId,
          workflowExecution: data.workflowExecution,
          payload: {
            activityType: data.activityType,
            input: JSON.parse(data.input)
          }
        };

        push(null, task);
      } catch(err) {
        console.warn(data.input, err);
      }

      return next();
    });

    // cancel requests when the stream ends so we're not hanging onto any
    // outstanding resources (swf.pollForActivityTask waits 60s for messages
    // by default)

    var abort = poll.abort.bind(poll);

    source.on("end", abort);

    // clean up event listeners
    poll.on("complete", _.partial(source.removeListener.bind(source), "end", abort));
  });

  if (fn) {
    source.pipe(new ActivityWorker(fn));

    worker.cancel = function() {
      source.destroy();
    };
  }

  return worker;
};
