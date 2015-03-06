"use strict";

var AWS = require("aws-sdk");

var argv = process.argv.slice(2);

if (argv.length !== 3) {
  console.warn("Usage: cancel.js <domain> <workflowId> <runId>");
  process.exit(1);
}

AWS.config.update({
  region: process.env.AWS_DEFAULT_REGION || "us-east-1"
});

var swf = new AWS.SWF();

swf.requestCancelWorkflowExecution({
  domain: argv.shift(),
  workflowId: argv.shift(),
  runId: argv.shift()
}, function(err, data) {
  if (err) {
    throw err;
  }

  process.stdout.write(JSON.stringify(data, null, "  "));
});
