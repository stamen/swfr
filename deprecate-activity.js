"use strict";

var AWS = require("aws-sdk");

var argv = process.argv.slice(2);

if (argv.length !== 3) {
  console.warn("Usage: status.js <domain> <workflowId> <runId>");
  process.exit(1);
}

AWS.config.update({
  region: process.env.AWS_DEFAULT_REGION || AWS.config.region || "us-east-1"
});

var swf = new AWS.SWF();

swf.describeWorkflowExecution({
  domain: argv.shift(),
  execution: {
    workflowId: argv.shift(),
    runId: argv.shift()
  }
}, function(err, data) {
  if (err) {
    throw err;
  }

  process.stdout.write(JSON.stringify(data, null, "  "));
});
