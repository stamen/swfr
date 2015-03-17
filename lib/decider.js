"use strict";

var assert = require("assert"),
    stream = require("stream"),
    util = require("util");

var AWS = require("aws-sdk"),
    P = require("bluebird");

var DeciderContext = require("./decider_context");

AWS.config.update({
  region: process.env.AWS_DEFAULT_REGION || "us-east-1"
});

var swf = new AWS.SWF();

var Decider = function(fn) {
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
    // console.log("Attempting to start.");
    //
    var chain = P.bind(context);

    P
      .bind(context)
      .then(fn.bind(context, chain, input)) // partially apply the worker fn w/ the input
      .catch(P.CancellationError, function() {
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

util.inherits(Decider, stream.Writable);

module.exports = Decider;
