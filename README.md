# swfr

AWS Simple Workflows using Node.js.

## Workflows

Workflows are constructed of 2 types of components: Activities and Deciders.
A workflow typically involves a single decider and 1 or more activities, each
of which can be executed independently.

### Decider

Deciders are responsible for orchestrating the workflow. This means that they
construct inputs for activities, determine which activities to execute (and
when), and what should be done with the output of those activities. Deciders
are intended to run quickly, distributing computationally-intensive tasks out
to activities.

Deciders are fairly complex, as they are required to consider the history of
all events that have occurred during the workflow in order to decide what the
next steps should be.

This is an example decider:

```javascript

```

### Activity

Activities represent discrete units of computation (or behavior). Given an
input, they produce an output.

There are two ways you can handle activites inside an activity worker.
One way is to pass in a function and manually handle the activities,
as is shown in the `examples/activities.js` example worker.

The second way is to write all of your activies in a folder, and then pass that directory
into the activity worker. The activity worker will then pull an activity to
execute from SWF, match it up to the activity in the provided activites folder,
and execute that code.

For instance, and activity worker script would look like this:

```javascript
"use strict";

var os = require("os");

var activity = require("swfr").activity,
    async = require("async"),
    env = require("require-env");
    
async.times(os.cpus().length, function(i) {
  return activity({
    domain: env.require("AWS_SWF_DOMAIN"),
    activitiesFolder: path.join(__dirname, "lib", "activities"),
    workerId: i
  });
}, function(err, workers) {
  if (err) {
    throw err;
  }

  process.on("SIGTERM", function() {
    return workers.forEach(function(w) {
      return w.cancel();
    });
  });
});
```

In this case, our activites live inside the `lib/activites` folder that exists under
the folder our activity worker is in. The above activity worker spins up a process that will
poll SWF for activities for each CPU on the machine. Once an activity is pulled of the task list,
swfr will match that activity by name and version to one in the `activityFolder` passed in.

This is an example activity that you would place into the `activitiesFolder`:

```javascript
"use strict";

module.exports = function echo(input, callback) {
  return callback(null, input);
};

module.exports.version = "1.0";
```

The activities must be registered by name and version inside of SWF.
The name of the activity is keyed off of the function name (meaning that it
can't be an anonymous function). The activity version is keyed off of
a `version` property present on the exported function object. These are used to
refer to the activity within SWF, so changes should be accompanied by
corresponding version bumps.

## Persisters

SWF payloads have a limit of 32kb; if inputs or outputs are larger than that,
swfr will use an out-of-band persister to store and transmit the payload. Right
now, the only persister that has been implemented uses DynamoDB and increases
the payload limit to 400kb.

The DynamoDB table used must be configured using `AWS_DYNAMODB_TABLE`.

## Running

```bash
# 1. install the AWS CLI

pip install awscli

# 2. register a "domain" to contain workflows:

aws swf register-domain --name <name> \
  --description <description> \
  --workflow-execution-retention-period-in-days <days>

# 3. confirm that the domain exists

aws swf list-domains --registration-status REGISTERED

# 4. register a workflow type for your workflow

aws swf register-workflow-type --domain <domain> \
  --name <name> \
  --workflow-version <version> \
  --default-task-list name=defaultTaskList \
  --default-task-start-to-close-timeout 30 \
  --default-execution-start-to-close-timeout 60 \
  --default-child-policy TERMINATE

# 5. check that the workflow type exists

aws swf list-workflow-types --domain <domain> \
  --registration-status REGISTERED

# 6. register activity types for each of your activities
# TODO do this according to name and version properties on activities

aws swf register-activity-type --domain <domain> \
  --name <name> \
  --activity-version <version> \
  --default-task-list name=defaultTaskList \
  --default-task-start-to-close-timeout 60 \
  --default-task-heartbeat-timeout 60 \
  --default-task-schedule-to-start-timeout 60 \
  --default-task-schedule-to-close-timeout 60

# 7. check that the activity types exist

aws swf list-activity-types --domain <domain> \
  --registration-status REGISTERED

# 8. start activity workers

node examples/activities.js

# 9. start decider worker

node examples/workflow.js

# 10. instantiate the workflow

aws swf start-workflow-execution --domain <domain> \
  --workflow-id <id> \
  --workflow-type name=<workflow name>,version=<workflow version>
```
