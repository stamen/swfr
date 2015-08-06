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

This is an example activity:

```javascript

```

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
