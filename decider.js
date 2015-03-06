"use strict";

var assert = require("assert"),
    EventEmitter = require("events").EventEmitter,
    os = require("os"),
    stream = require("stream"),
    util = require("util");

var _ = require("highland"),
    AWS = require("aws-sdk"),
    Promise = require("bluebird"),
    retry = require("bluebird-retry");

AWS.config.update({
  region: process.env.AWS_DEFAULT_REGION || "us-east-1"
});

var swf = new AWS.SWF();

var newCancellablePromise = function() {
  return new Promise(function(resolve) {
    // resolve asynchronously, giving us a chance to cancel it
    return setImmediate(resolve);
  }).cancellable();
};

var DecisionContext = function(task) {
  this.decisions = [];
  this.task = task;
  // we're replaying at first if this isn't the first time through the workflow
  this.replaying = this.task.previousStartedEventId !== 0;
  this.activities = {};

  this.history = this.task.events.map(function(event) {
    switch (event.eventType) {
    case "ActivityTaskScheduled":
      this.activities[event.eventId] = {
        attributes: event.activityTaskScheduledEventAttributes,
        status: "scheduled",
        lastEventId: event.eventId
      };

      // the order in which events were scheduled matters (since it matches the
      // local workflow)
      return event.eventId;

    case "ActivityTaskStarted":
      var eventId = event.activityTaskStartedEventAttributes.scheduledEventId;

      this.activities[eventId].status = "started";
      this.activities[eventId].lastEventId = event.eventId;

      break;

    case "ActivityTaskCompleted":
      var attrs = event.activityTaskCompletedEventAttributes,
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
      var attrs = event.activityTaskFailedEventAttributes,
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

    case "WorkflowExecutionStarted":
    case "DecisionTaskScheduled":
    case "DecisionTaskStarted":
    case "DecisionTaskCompleted":
      // noop

      break;

    default:
      console.log("Unimplemented handler for", event.eventType);

    // TODO
    // "ScheduleActivityTaskFailed"
    // "ActivityTaskFailed"
    // "ActivityTaskTimedOut"
    // "ActivityTaskCanceled"
    // "ActivityTaskCancelRequested"
    // "RequestCancelActivityTaskFailed"
    }
  }.bind(this)).filter(function(id) {
    // filter out nulls
    return !!id;
  });

  // console.log("history:", this.history);
  // console.log("activities:", this.activities);
};

DecisionContext.prototype.activity = function(name, version) {
  var args = Array.prototype.slice.call(arguments, 2),
      context = this;

  // console.log("activity: %s (%s):", name, version, args);

  args = JSON.stringify(args);

  return new Promise(function(resolve, reject) {
    if (context.history.length > 0) {
      var entry = context.activities[context.history[0]];

      // we're replaying if the task hasn't changed since the last time we
      // attempted the workflow
      context.replaying = entry.lastEventId <= context.task.previousStartedEventId;

      if (entry.attributes.activityType.name === name &&
          entry.attributes.activityType.version === version &&
          entry.attributes.input === args) {
        context.history.shift();
        // console.log("using history:", entry);

        switch (entry.status) {
        case "completed":
          return resolve(entry.result);

        case "failed":
          return reject(entry.error);

        case "scheduled":
        case "started":
          // console.log("status:", entry.status);

          var promise = newCancellablePromise();

          // pass this promise forward and immediately cancel it (to end the chain)
          resolve(promise);
          return promise.cancel();

        default:
          console.warn("Unsupported status:", entry.status);

          var promise = newCancellablePromise();

          // pass this promise forward and immediately cancel it (to end the chain)
          resolve(promise);
          return promise.cancel();
        }

        return resolve(entry.result);
      }

      // TODO bubble this up to here (not the workflow)
      return reject(new Error(util.format("Unexpected entry in history:", entry)));
    }

    // we're definitely not replaying now
    context.replaying = false;

    // do the thing
    // console.log("Calling %s[%s](%s)", name, version, args);

    // append to decisions
    context.decisions.push({
      decisionType: "ScheduleActivityTask",
      scheduleActivityTaskDecisionAttributes: {
        activityId: Date.now().toString(), // TODO UUID
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
      }
    });

    var promise = newCancellablePromise();

    // pass this promise forward and immediately cancel it (to end the chain)
    resolve(promise);
    return promise.cancel();
  });
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
