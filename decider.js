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
  this.activities = {};
  this.rescheduled = {};

  this.task.events.forEach(function(event) {
    var eventId = event.eventId,
        attrs;

    switch (event.eventType) {
    case "ActivityTaskScheduled":
      // activities appear to be scheduled in order even if they're not
      // necessarily executed in order
      attrs = event.activityTaskScheduledEventAttributes;

      // console.log("activity task scheduled: %j", attrs);

      this.activities[eventId] = {
        attributes: attrs,
        status: "scheduled",
        lastEventId: eventId
      };

      // index by activity id
      var key = JSON.stringify({
        name: attrs.activityType.name,
        version: attrs.activityType.version,
        input: attrs.input
      });

      this.activities[key] = eventId;

      break;

    case "ActivityTaskStarted":
      eventId = event.activityTaskStartedEventAttributes.scheduledEventId;

      // console.log("activity task started: %j", event);

      this.activities[eventId].status = "started";
      this.activities[eventId].lastEventId = event.eventId;

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
      // since the desired task doesn't show up in the history, it will be
      // rescheduled on the next run-through

      attrs = event.scheduleActivityTaskFailedEventAttributes;

      // mark this activity as rescheduled
      this.rescheduled[attrs.activityId] = eventId;

      this.activities[eventId] = {
        attributes: attrs,
        status: "schedule-failed",
        lastEventId: eventId,
        error: new Error(attrs.cause)
      };

      console.warn("Schedule activity task failed:", attrs);

      break;

    case "WorkflowExecutionStarted":
    case "DecisionTaskScheduled":
    case "DecisionTaskStarted":
    case "DecisionTaskCompleted":
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

  // console.log("activities:", this.activities);
};

DecisionContext.prototype.activity = function() {
  var options = {
    // defaults go here
  };

  var runActivity = function(name, version) {
    var args = Array.prototype.slice.call(arguments, 2),
        context = this;

    // console.log("activity: %s (%s):", name, version, args);
    // console.log("options:", options);

    args = JSON.stringify(args);

    return new Promise(function(resolve, reject) {
      var key = JSON.stringify({
        name: name,
        version: version,
        input: args
      });

      var eventId = context.activities[key];

      if (context.activities[eventId]) {
        var entry = context.activities[eventId];

        // we're replaying if the task hasn't changed since the last time we
        // attempted the workflow
        context.replaying = entry.lastEventId <= context.task.previousStartedEventId;

        if (entry.attributes.activityType.name === name &&
            entry.attributes.activityType.version === version &&
            entry.attributes.input === args) {

          switch (entry.status) {
          case "completed":
            return resolve(entry.result);

          case "failed":
          case "timeout":
            return reject(entry.error);

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
        } else {
          // TODO bubble this up to here (not the workflow)
          return reject(new Error(util.format("Unexpected entry in history:", entry)));
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
    // console.log("Task:", task);
    // pass the activity type + input to the function and provide the rest as
    // the context
    var context = new DecisionContext(task);

    // console.log("Attempting to start.");

    Promise
      .resolve()
      .bind(context)
      .then(fn.bind(context, task.payload)) // partially apply the worker fn w/ the payload
      .catch(Promise.CancellationError, function(err) {
        // console.warn("Chain interrupted:", err);
      })
      .catch(function(err) {
        console.warn("Error in workflow:", err.stack);
      })
      .finally(function() {
        // console.log("decisions:", this.decisions);
        // console.log("done (with this attempt).");

        // send decisions

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

      try {
        // console.log("Response: %j", data);

        // var task = {
        //   domain: options.domain,
        //   taskList: options.taskList,
        //   eventId: data.eventId,
        //   taskToken: data.taskToken,
        //   previousStartedEventId: data.previousStartedEventId,
        //   startedEventId: data.startedEventId,
        //   workflowExecution: data.workflowExecution,
        //   nextPageToken: data.nextPageToken,
        //   payload: {
        //     workflowType: data.workflowType
        //   }
        // };

        push(null, data);
      } catch (err) {
        console.warn(data.input, err);
      }

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
