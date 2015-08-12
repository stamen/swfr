"use strict";

var assert = require("assert"),
    EventEmitter = require("events").EventEmitter,
    https = require("https"),
    os = require("os"),
    stream = require("stream"),
    util = require("util");

var _ = require("highland"),
    AWS = require("aws-sdk");

var payloadPersister = require("./payload-persister");

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
    var payload = clone(task.payload),
        heartbeat = function() {
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
    return payloadPersister.load(task.payload.input, function(err, input) {
      if (err) {
        // cancel the reservation extention
        clearInterval(extension);

        console.warn(err.stack);
        return callback();
      }

      payload.input = input;

      // pass the activity type + input to the function and provide the rest as
      // the context
      return fn.call(task, payload, function(err, result) {
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

        return payloadPersister.save(task.taskToken, result, function(err, result) {
          if (err) {
            console.warn(err.stack);
            return callback();
          }

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
    });
  };
};

util.inherits(ActivityWorker, stream.Writable);

module.exports = ActivityWorker;
