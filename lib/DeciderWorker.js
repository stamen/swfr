"use strict";

var assert = require("assert"),
    stream = require("stream"),
    util = require("util");

var AWS = require("aws-sdk"),
    Promise = require("bluebird");

var DeciderContext = require("./DeciderContext");

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
    var context = new DeciderContext(task),
        input;

    assert.equal("WorkflowExecutionStarted",
                 task.events[0].eventType,
                 "Expected first event to be of type WorkflowExecutionStarted");

    try {
      input = JSON.parse(task.events[0].workflowExecutionStartedEventAttributes.input);
    } catch (err) {
      console.warn("Malformed input:", input, err);
    }

    var chain = Promise.bind(context);

    Promise
      .bind(context)
      .then(fn.bind(context, chain, input)) // partially apply the worker fn w/ the input
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
  };
};

util.inherits(DeciderWorker, stream.Writable);

module.exports = DeciderWorker;
