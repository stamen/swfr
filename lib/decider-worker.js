"use strict";

var assert = require("assert"),
    stream = require("stream"),
    util = require("util");

var AWS = require("aws-sdk"),
    Promise = require("bluebird");

var DeciderContext = require("./decider-context");

AWS.config.update({
  region: process.env.AWS_DEFAULT_REGION || AWS.config.region || "us-east-1"
});

var swf = new AWS.SWF();

var DeciderWorker = function(fn) {
  stream.Writable.call(this, {
    objectMode: true,
    highWaterMark: 1 // limit the number of buffered tasks
  });

  this._write = function(task, encoding, callback) {
    // pass the input to the function and provide the rest as the context
    var context = new DeciderContext(task);

    assert.equal("WorkflowExecutionStarted",
                 task.events[0].eventType,
                 "Expected first event to be of type WorkflowExecutionStarted");

    var chain = Promise.bind(context);

    // apply a hard cap to .map concurrency (matching SWF limits for
    // concurrently scheduled/running activities)
    var _map = chain.map;

    chain.map = function(fn, options) {
      options = options || {};
      options.concurrency = Math.min(options.concurrency, 1000) || 1000;

      return _map.apply(this, fn, options);
    };

    try {
      Promise
        .bind(context)
        // .then(function() {
        //   // has enough changed that warrants the decider to be re-run?
        //   var taskDelta = task.events.slice((task.previousStartedEventId || 1) - 1, task.startedEventId);
        //
        //   var newlyCompletedTasks = taskDelta.filter(function(x) {
        //     // TODO there should be more here
        //     return ["WorkflowExecutionStarted", "ActivityTaskCompleted"].indexOf(x.eventType) >= 0;
        //   });
        //
        //   if (newlyCompletedTasks.length === 0) {
        //     console.log("Passed-over event types:", taskDelta.map(function(x) { return x.eventType }));
        //     // nothing substantive changed; cancel the pipeline
        //     throw new Promise.CancellationError();
        //   }
        // })
        .then(function() {
          if (task.events[0].workflowExecutionStartedEventAttributes.input) {
            return JSON.parse(task.events[0].workflowExecutionStartedEventAttributes.input);
          }
        })
        .then(fn.bind(context, chain)) // partially apply the worker fn
        .catch(Promise.CancellationError, function() {
          // workflow couldn't run to completion; this is not an error
        })
        .catch(function(err) {
          this.decisions.push({
            decisionType: "FailWorkflowExecution",
            failWorkflowExecutionDecisionAttributes: {
              details: JSON.stringify({
                payload: err.payload,
                stack: err.stack
              }),
              reason: err.message
            }
          });
        })
        .finally(function() {
          // we're done deciding on next actions; report back

          return swf.respondDecisionTaskCompleted({
            taskToken: task.taskToken,
            decisions: this.decisions,
            executionContext: this.status
          }, function(err) {
            if (err) {
              console.warn(err.stack);
            }

            return callback();
          });
        });
    } catch (err) {
      console.warn(err.stack);
      return callback(err);
    }
  };
};

util.inherits(DeciderWorker, stream.Writable);

module.exports = DeciderWorker;
