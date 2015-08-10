"use strict";

var assert = require("assert"),
    crypto = require("crypto");

var debug = require("debug")("swfr:decider_context"),
    Promise = require("bluebird"),
    humanize = require("humanize-plus");

var payloadPersister = require("./payload-persister");

var cancel = function(resolve) {
  var promise = Promise.pending().promise.cancellable();

  resolve(promise);

  return promise.cancel();
};

var sha512 = function(obj) {
  var hashStream = crypto.createHash("sha512");

  hashStream.end(JSON.stringify(obj));

  return hashStream.read().toString("hex");
};

var DeciderContext = function(task) {
  this.decisions = [];
  this.task = task;
  this.userData = {};
  // we're replaying at first if this isn't the first time through the workflow
  this.replaying = this.task.previousStartedEventId !== 0;

  // information about activities (and pointers to them)
  this.activities = {};

  // Initialize the payload persisters.
  payloadPersister.init();

  this.task.events.forEach(function(event) {
    var eventId = event.eventId,
        attrs;

    switch (event.eventType) {
    case "ActivityTaskScheduled":
      attrs = event.activityTaskScheduledEventAttributes;

      // index by activity id (set when scheduled)
      var input = JSON.parse(attrs.input),
          key = input.key,
          attempts = 0;

      assert.ok(key, "Key must be defined.");

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

      if (attrs.result) {
        try {
          this.activities[eventId].result = JSON.parse(attrs.result);
        } catch (err) {
          console.warn("Error parsing activity result:", attrs.result, err);
        }
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
      // this occurs when re-using ids, when default timeouts weren't provided
      // (and overrides aren't present), and for other reasons

      attrs = event.scheduleActivityTaskFailedEventAttributes;

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

DeciderContext.prototype.activity = function() {
  var options = {
    // defaults go here
    retries: 1,
    taskList: "defaultTaskList"
  };

  var runActivity = function(name, version) {
    var args = Array.prototype.slice.call(arguments, 2),
        context = this,
        // hash key components to limit size
        key = sha512({
          name: name,
          version: version,
          input: JSON.stringify(args)
        }),
        payload = {
          args: args,
          key: key
        };

    // have we heard about this activity?
    // (set this for the benefit of log())
    context.replaying = !!context.activities[key];

    return new Promise(function(resolve, reject) {
      var eventId = context.activities[key];

      if (context.activities[eventId]) {
        var entry = context.activities[eventId];

        // we're replaying if the task hasn't changed since the last time we
        // attempted the workflow
        context.replaying = entry.lastEventId <= context.task.previousStartedEventId;

        switch (entry.status) {
        case "completed":
          return payloadPersister.load(entry.result, function(err, payload) {
            if (err) {
              console.warn("Error loading persisted result:", err);
              return cancel(resolve);
            }

            return resolve(payload);
          });

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

      // hash the attributes to give us predictable activity ids
      // NOTE: also prevents duplicate activities
      var activityId = sha512({
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
        input: payload.args,
        taskList: {
          name: options.taskList
        }
      });

      return payloadPersister.save(activityId, payload.args, function(err, args) {
        if (err) {
          console.warn("Error persisting payload:", err);
          return cancel(resolve);
        }

        payload.args = args;

        var attrs = {
          activityId: activityId,
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
          input: JSON.stringify(payload),
          taskList: {
            name: options.taskList
          }
        };

        // append to decisions
        context.decisions.push({
          decisionType: "ScheduleActivityTask",
          scheduleActivityTaskDecisionAttributes: attrs
        });

        // allow time for multiple promises to be returned (from .map, for
        // example)
        return setTimeout(function() {
          return cancel(resolve);
        }, 100);
      });
    });
  }.bind(this);

  // support for options as the first invocation, e.g.:
  //   this.activity({ retries: 2 })("name", "version")
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

DeciderContext.prototype.complete = function(result) {
  // console.log("Completion requested.");
  this.decisions.push({
    decisionType: "CompleteWorkflowExecution",
    completeWorkflowExecutionDecisionAttributes: {
      result: result
    }
  });
};

DeciderContext.prototype.log = function() {
  // only do this if we're not replaying
  // NOTE: if the containing activity has not yet been scheduled, this may
  // output repeatedly
  if (!this.replaying) {
    console.log.apply(null, arguments);
  }
};

module.exports = DeciderContext;
