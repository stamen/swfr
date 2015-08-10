"use strict";

var assert = require("assert"),
    EventEmitter = require("events").EventEmitter,
    https = require("https"),
    os = require("os"),
    util = require("util");

var _ = require("highland"),
    AWS = require("aws-sdk");

var activityHandler = require("./activity-handler"),
    ActivityWorker = require("./lib/activity-worker");

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

/**
 * Available options:
 * * domain - Workflow domain (required)
 * * taskList - Task list
 * * activitiesFolder - If no fn is specified, use an activityHandler
 * *                    who's activities are in this folder.
 * * workerId - Worker ID, for debugging.
 */
module.exports = function(options, fn) {
  assert.ok(options.domain, "options.domain is required");

  if(!fn) {
    assert.ok(options.activitiesFolder, "options.activitiesFolder required if no handler specified");
    fn = activityHandler(options.activitiesFolder, options.workerId);
  }

  options.taskList = options.taskList || "defaultTaskList";

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
