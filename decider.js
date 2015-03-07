"use strict";

var assert = require("assert"),
    crypto = require("crypto"),
    EventEmitter = require("events").EventEmitter,
    os = require("os"),
    stream = require("stream"),
    util = require("util");

var _ = require("highland"),
    AWS = require("aws-sdk"),
    Promise = require("bluebird");

AWS.config.update({
  region: process.env.AWS_DEFAULT_REGION || "us-east-1"
});

var swf = new AWS.SWF();

var cancel = function(resolve) {
  var promise = Promise.pending().promise.cancellable();

  resolve(promise);

  return promise.cancel();
};

var DecisionContext = function(task) {
  this.decisions = [];
  this.task = task;
  // we're replaying at first if this isn't the first time through the workflow
  this.replaying = this.task.previousStartedEventId !== 0;

  // information about activities (and pointers to them)
  this.activities = {};

  this.task.events.forEach(function(event) {
    var eventId = event.eventId,
        attrs;

    switch (event.eventType) {
    case "ActivityTaskScheduled":
      attrs = event.activityTaskScheduledEventAttributes;

      // index by activity id
      var key = JSON.stringify({
        name: attrs.activityType.name,
        version: attrs.activityType.version,
        input: attrs.input
      });

      var attempts = 0;

      if (this.activities[key]) {
        // this is a retry; copy attempts from the previous one
        attempts = this.activities[this.activities[key]].attempts;

        // clear out the previous attempt
        delete this.activities[this.activities[key]];
      }

      // create a new activity entry
      this.activities[eventId] = {
        attributes: attrs,
        status: "scheduled",
        lastEventId: eventId,
        key: key,
        attempts: attempts
      };

      // update the reference
      this.activities[key] = eventId;

      break;

    case "ActivityTaskStarted":
      eventId = event.activityTaskStartedEventAttributes.scheduledEventId;

      this.activities[eventId].status = "started";
      this.activities[eventId].lastEventId = event.eventId;
      this.activities[eventId].attempts++;

      break;

    case "ActivityTaskCompleted":
      attrs = event.activityTaskCompletedEventAttributes;
      eventId = attrs.scheduledEventId;

      this.activities[eventId].status = "completed";
      this.activities[eventId].lastEventId = event.eventId;

      try {
        this.activities[eventId].result = JSON.parse(attrs.result);
      } catch (err) {
        console.warn(attrs.result, err);
      }

      break;

    case "ActivityTaskFailed":
      attrs = event.activityTaskFailedEventAttributes;
      eventId = attrs.scheduledEventId;

      this.activities[eventId].status = "failed";
      this.activities[eventId].reason = attrs.reason;

      try {
        var details = JSON.parse(attrs.details),
            error = new Error(attrs.reason);

        // copy properties over to the error
        Object.keys(details).forEach(function(k) {
          error[k] = details[k];
        });

        // provide a full-fledged error to reject with
        this.activities[eventId].error = error;
      } catch (err) {
        console.warn(attrs.details, err);
      }

      break;

    case "ActivityTaskTimedOut":
      attrs = event.activityTaskTimedOutEventAttributes;
      eventId = attrs.scheduledEventId;

      this.activities[eventId].status = "timeout";
      this.activities[eventId].error = new Error(attrs.timeoutType);

      break;

    case "ScheduleActivityTaskFailed":
      // this occurs when re-using ids (or other reasons?)

      attrs = event.scheduleActivityTaskFailedEventAttributes;

      // TODO does this still work?

      this.activities[eventId] = {
        attributes: attrs,
        status: "schedule-failed",
        lastEventId: eventId,
        error: new Error(attrs.cause)
      };

      console.warn("Schedule activity task failed:", event);

      break;

    case "FailWorkflowExecutionFailed":
      // TODO figure out how to handle this
      console.log("%j", event);
      break;

    case "WorkflowExecutionStarted":
    case "DecisionTaskScheduled":
    case "DecisionTaskStarted":
    case "DecisionTaskCompleted":
    case "DecisionTaskTimedOut":
      // noop

      break;

    default:
      console.log("Unimplemented handler for", event.eventType);

    // TODO
    // "ActivityTaskCanceled"
    // "ActivityTaskCancelRequested"
    // "RequestCancelActivityTaskFailed"
    }
  }.bind(this));
};

DecisionContext.prototype.activity = function() {
  var options = {
    // defaults go here
    retries: 1
  };

  var runActivity = function(name, version) {
    var args = Array.prototype.slice.call(arguments, 2),
        context = this,
        key = JSON.stringify({
          name: name,
          version: version,
          input: JSON.stringify(args)
        });

    args = JSON.stringify(args);

    return new Promise(function(resolve, reject) {
      var eventId = context.activities[key];

      if (context.activities[eventId]) {
        var entry = context.activities[eventId];

        // we're replaying if the task hasn't changed since the last time we
        // attempted the workflow
        context.replaying = entry.lastEventId <= context.task.previousStartedEventId;

        switch (entry.status) {
        case "completed":
          return resolve(entry.result);

        case "failed":
        case "timeout":
          if (entry.attempts >= options.retries) {
            // out of retries
            return reject(entry.error);
          }

          break;

        case "scheduled":
        case "started":
          // cancel the workflow; we can't fulfill this promise on this run

          return cancel(resolve);

        case "schedule-failed":
          // break out and attempt to schedule the task again
          break;

        default:
          console.warn("Unsupported status:", entry.status);

          return cancel(resolve);
        }
      }

      // we're definitely not replaying now
      context.replaying = false;

      // do the thing
      // console.log("Calling %s[%s](%s)", name, version, args);

      var attrs = {
            activityType: {
              name: name,
              version: version
            },
            // TODO control
            // TODO heartbeatTimeout
            // TODO scheduleToCloseTimeout
            // TODO scheduleToStartTimeout
            // TODO startToCloseTimeout
            // TODO taskPriority
            input: args,
            taskList: {
              name: "splitmerge_activity_tasklist" // TODO
            }
          },
          // hash the attributes to give us predictable activity ids
          // NOTE: also prevents duplicate activities
          hashStream = crypto.createHash("sha512");

      hashStream.end(JSON.stringify(attrs));
      attrs.activityId = hashStream.read().toString("hex");

      // append to decisions
      context.decisions.push({
        decisionType: "ScheduleActivityTask",
        scheduleActivityTaskDecisionAttributes: attrs
      });

      return cancel(resolve);
    });
  }.bind(this);

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

DecisionContext.prototype.complete = function(result) {
  // console.log("Completion requested.");
  this.decisions.push({
    decisionType: "CompleteWorkflowExecution",
    completeWorkflowExecutionDecisionAttributes: {
      result: result
    }
  });
};

DecisionContext.prototype.log = function() {
  // only do this if we're not replaying
  if (!this.replaying) {
    console.log.apply(null, arguments);
  }
};

var DecisionWorker = function(fn) {
  stream.Writable.call(this, {
    objectMode: true,
    highWaterMark: 1 // limit the number of buffered tasks
  });

  this._write = function(task, encoding, callback) {
    // pass the input to the function and provide the rest as the context
    var context = new DecisionContext(task),
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

    Promise
      .resolve()
      .bind(context)
      .then(fn.bind(context, input)) // partially apply the worker fn w/ the input
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

util.inherits(DecisionWorker, stream.Writable);

/**
 * Available options:
 * * domain - Workflow domain (required)
 * * taskList - Task list (required)
 */
module.exports = function(options, fn) {
  assert.ok(options.domain, "options.domain is required");
  assert.ok(options.taskList, "options.taskList is required");

  var worker = new EventEmitter();

  var source = _(function(push, next) {
    // TODO note: activity types need to be registered in order for workflow
    // executions to not fail

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

      push(null, data);

      return next();
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
    source.pipe(new DecisionWorker(fn));

    worker.cancel = function() {
      source.destroy();
    };
  }

  return worker;
};
